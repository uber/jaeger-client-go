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
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestJitterBackOff(t *testing.T) {
	type args struct {
		duration time.Duration
		i        uint
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			"jitterBackOff-second-0",
			args{time.Second, 0},
			time.Second,
		},
		{
			"jitterBackOff-milliseconds-100",
			args{time.Millisecond * 100, 0},
			time.Millisecond * 100,
		},
		{
			"jitterBackOff-milliseconds-0",
			args{time.Millisecond * 500, 0},
			time.Millisecond * 500,
		},
		{
			"jitterBackOff-milliseconds-1",
			args{time.Millisecond * 500, 1},
			time.Second,
		},
		{
			"jitterBackOff-milliseconds-2",
			args{time.Millisecond * 500, 2},
			time.Second * 2,
		},
		{
			"jitterBackOff-minute-0",
			args{time.Minute, 0},
			time.Minute,
		},
		{
			"jitterBackOff-max",
			args{time.Second, 10},
			MaxInterval,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := jitterBackOff(tt.args.duration, tt.args.i)
			assert.True(t, assert.InDelta(t, got, tt.want, float64(tt.want)*1.0/3, "timer not excatlt"))
		})
	}
}

func TestTickerOpertionSuccess(t *testing.T) {
	count := 5
	delta := time.Millisecond * 20
	operation := func() error {
		return nil
	}
	t0 := time.Now()
	ticker := NewTicker(delta)
	for j := 0; j < count; j++ {
		<-ticker.C
		err := operation()
		ticker.UpdateTimer(err)
	}
	ticker.Stop()
	t1 := time.Now()
	dt := t1.Sub(t0)
	target := delta * time.Duration(count)
	slop := target * 5 / 10
	if dt < target-slop || dt > target+slop {
		t.Errorf("ticks took %s, expected [%s,%s]", delta, target-slop, target+slop)
	}
}

func TestTickerOpertionFail(t *testing.T) {
	count := 5
	delta := time.Millisecond * 20
	operation := func() error {
		return fmt.Errorf("operation fail")
	}
	t0 := time.Now()
	ticker := NewTicker(delta)
	//backoff cnt
	backCnt := 1 + 2 + 4 + 8
	for j := 0; j < count; j++ {
		<-ticker.C
		err := operation()
		ticker.UpdateTimer(err)
	}
	ticker.Stop()
	t1 := time.Now()
	dt := t1.Sub(t0)
	target := delta * time.Duration(backCnt)
	slop := target * 5 / 10
	if dt < target-slop || dt > target+slop {
		t.Errorf("ticks took %s, expected [%s,%s]", dt, target-slop, target+slop)
	}
}

func TestTickerOpertionNormal(t *testing.T) {
	count := 7
	delta := time.Millisecond * 20
	operation := func(i int) error {
		if i <= 1 {
			return nil
		}
		if i <= 4 {
			return fmt.Errorf("operation fail")
		}
		return nil
	}
	t0 := time.Now()
	ticker := NewTicker(delta)
	//first two success,  then third error, end with two success operation
	backCnt := 1 + 1 + 1 + 2 + 4 + 1 + 1
	for j := 0; j < count; j++ {
		<-ticker.C
		err := operation(j)
		ticker.UpdateTimer(err)
	}
	ticker.Stop()
	t1 := time.Now()
	dt := t1.Sub(t0)
	target := delta * time.Duration(backCnt)
	slop := target * 5 / 10
	if dt < target-slop || dt > target+slop {
		t.Errorf("ticks took %s, expected [%s,%s]", dt, target-slop, target+slop)
	}
}
