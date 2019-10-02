// Copyright (c) 2017 Uber Technologies, Inc.
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

package utils

import (
	"github.com/uber/jaeger-client-go/internal/ratelimiter"
)

// RateLimiter is a filter used to check if a message that is worth itemCost units is within the rate limits.
//
// Deprecated: RateLimiter interface will be removed in v3.0.0
type RateLimiter interface {
	CheckCredit(itemCost float64) bool
}

// NewRateLimiter creates a new rate limiter based on leaky bucket algorithm, formulated in terms of a
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
//
// Deprecated: NewRateLimiter will no be exposed as a public API in v3.0.0
func NewRateLimiter(creditsPerSecond, maxBalance float64) RateLimiter {
	return ratelimiter.New(creditsPerSecond, maxBalance, ratelimiter.Options{})
}
