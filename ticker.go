// Copyright (c) 2021 The Jaeger Authors.
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

package jaeger

import (
	"math/rand"
	"time"
)

var random *rand.Rand

func init() {
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// Ticker  is a customer Ticker, it perform like a normal time.Ticker when operation is success,
// When Operation is fail, it will use exponential backoff with a jitter.
type Ticker struct {
	C        <-chan time.Time
	timer    *time.Timer
	duration time.Duration
	failCnt  uint
}

// NewTicker construct a new Ticker
func NewTicker(duratin time.Duration) *Ticker {
	t := &Ticker{
		timer:    time.NewTimer(duratin),
		duration: duratin,
	}
	t.C = t.timer.C
	return t
}

// Stop turns off a Ticker. After Stop, no more ticks will be sent.
func (t *Ticker) Stop() {
	t.timer.Stop()
}

// UpdateTimer update the timer use operation result, must be called after every operation
func (t *Ticker) UpdateTimer(err error) {
	d := t.next(err)
	t.timer.Reset(d)
	t.C = t.timer.C
}

func (t *Ticker) next(err error) time.Duration {
	if err == nil {
		t.failCnt = 0
		return t.duration
	}
	t.failCnt++
	return jitterBackOff(t.duration, t.failCnt-1)
}

//MaxInterval max duration for the timer
const MaxInterval = time.Minute * 5

// jitterBackOff returns ever increasing backoffs by a power of 2 util reach the maxInterval
// with +/- 0-33% to prevent sychronized reuqests.
func jitterBackOff(duration time.Duration, i uint) time.Duration {
	backoff := jitter(duration, 1<<i)
	if backoff > MaxInterval {
		return MaxInterval
	}
	return backoff
}

// jitter keeps the +/- 0-33% logic in one place
func jitter(duration time.Duration, i uint) time.Duration {
	interval := int64(duration) * int64(i)
	//return time.Duration(interval)
	delta := interval / 3
	if delta <= 0 {
		return time.Duration(interval)
	}
	// interval ± rand
	interval += random.Int63n(2*delta) - delta
	// a jitter of 0 messes up the time.Tick chan
	if interval <= 0 {
		interval = 1
	}
	return time.Duration(interval)
}
