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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateLimiter(t *testing.T) {
	limiter := New(2.0, 2.0, Options{RandomNumber: func() float64 { return 1.0 }})
	// stop time
	ts := time.Now()
	limiter.lastTick = ts
	limiter.options.TimeNow = func() time.Time {
		return ts
	}
	assert.True(t, limiter.CheckCredit(1.0))
	assert.True(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
	// move time 250ms forward, not enough credits to pay for 1.0 item
	limiter.options.TimeNow = func() time.Time {
		return ts.Add(time.Second / 4)
	}
	assert.False(t, limiter.CheckCredit(1.0))
	// move time 500ms forward, now enough credits to pay for 1.0 item
	limiter.options.TimeNow = func() time.Time {
		return ts.Add(time.Second/4 + time.Second/2)
	}
	assert.True(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
	// move time 5s forward, enough to accumulate credits for 10 messages, but it should still be capped at 2
	limiter.lastTick = ts
	limiter.options.TimeNow = func() time.Time {
		return ts.Add(5 * time.Second)
	}
	assert.True(t, limiter.CheckCredit(1.0))
	assert.True(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
}

func TestMaxBalance(t *testing.T) {
	limiter := New(0.1, 1.0, Options{RandomNumber: func() float64 { return 1.0 }})
	// stop time
	ts := time.Now()
	limiter.lastTick = ts
	limiter.options.TimeNow = func() time.Time {
		return ts
	}
	// on initialization, should have enough credits for 1 message
	assert.True(t, limiter.CheckCredit(1.0))

	// move time 20s forward, enough to accumulate credits for 2 messages, but it should still be capped at 1
	limiter.options.TimeNow = func() time.Time {
		return ts.Add(time.Second * 20)
	}
	assert.True(t, limiter.CheckCredit(1.0))
	assert.False(t, limiter.CheckCredit(1.0))
}

func TestUpdate(t *testing.T) {
	limiter := New(2.0, 2.0, Options{RandomNumber: func() float64 { return 1.0 }})
	// stop time
	ts := time.Now()
	limiter.lastTick = ts
	limiter.options.TimeNow = func() time.Time {
		return ts
	}
	assert.True(t, limiter.CheckCredit(1.0))
	limiter.Update(1.0, 1.0)

	assert.Equal(t, 0.5, limiter.balance)
	assert.False(t, limiter.CheckCredit(1.0))

	limiter.Update(2.0, 2.0)
	assert.Equal(t, 1.0, limiter.balance)
	assert.True(t, limiter.CheckCredit(1.0))
}
