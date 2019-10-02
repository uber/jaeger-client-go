// Copyright (c) 2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ratelimiter

import (
	"math/rand"
	"sync"
	"time"
)

// RateLimiter is a leaky bucket implementation of a RateLimiter.
type RateLimiter struct {
	lock             sync.Mutex
	options          Options
	creditsPerSecond float64
	balance          float64
	maxBalance       float64
	lastTick         time.Time
}

// Options control behavior of the rate limiter.
type Options struct {
	RandomNumber func() float64
	TimeNow      func() time.Time
}

func (o *Options) applyDefaults() {
	if o.RandomNumber == nil {
		o.RandomNumber = rand.Float64
	}
	if o.TimeNow == nil {
		o.TimeNow = time.Now
	}
}

// New creates a new rate limiter based on leaky bucket algorithm, formulated in terms of a
// credits balance that is replenished every time CheckCredit() method is called (tick) by the amount proportional
// to the time elapsed since the last tick, up to max of creditsPerSecond. A call to CheckCredit() takes a cost
// of an item we want to pay with the balance. If the balance exceeds the cost of the item, the item is "purchased"
// and the balance reduced, indicated by returned value of true. Otherwise the balance is unchanged and return false.
//
// This can be used to limit a rate of messages emitted by a service by instantiating the Rate Limiter with the
// max number of messages a service is allowed to emit per second, and calling CheckCredit(1.0) for each message
// to determine if the message is within the rate limit.
//
// It can also be used to limit the rate of traffic in bytes, by setting creditsPerSecond to desired throughput
// as bytes/second, and calling CheckCredit() with the actual message size.
func New(creditsPerSecond, maxBalance float64, options Options) *RateLimiter {
	(&options).applyDefaults()
	return &RateLimiter{
		creditsPerSecond: creditsPerSecond,
		balance:          maxBalance * options.RandomNumber(),
		maxBalance:       maxBalance,
		lastTick:         time.Now(),
		options:          options,
	}
}

// Update updates the rate limiter with new configurations. This allows the balance to carry over as the rate changes.
func (b *RateLimiter) Update(creditsPerSecond, maxBalance float64) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.updateBalance()
	b.creditsPerSecond = creditsPerSecond
	// The new balance should be proportional to the old balance.
	b.balance = maxBalance * b.balance / b.maxBalance
	b.maxBalance = maxBalance
}

// CheckCredit returns true if the RateLimiter has enough credit balance for the itemCost.
// TODO Change API to accept the timestamp here, since it's always available when starting the span.
func (b *RateLimiter) CheckCredit(itemCost float64) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.updateBalance()
	// if we have enough credits to pay for current item, then reduce balance and allow
	if b.balance >= itemCost {
		b.balance -= itemCost
		return true
	}
	return false
}

// NB: this function should only be called while holding the lock
func (b *RateLimiter) updateBalance() {
	// calculate how much time passed since the last tick, and update current tick
	currentTime := b.options.TimeNow()
	elapsedTime := currentTime.Sub(b.lastTick)
	b.lastTick = currentTime
	// calculate how much credit have we accumulated since the last tick
	b.balance += elapsedTime.Seconds() * b.creditsPerSecond
	if b.balance > b.maxBalance {
		b.balance = b.maxBalance
	}
}
