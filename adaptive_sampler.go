// Copyright (c) 2018 The Jaeger Authors.
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
	"sync"
	"time"
	"github.com/uber/jaeger-client-go/utils"
	"math"
)

const (
	defaultProbability = 0.001
)

// struct for debugging purposes only, keeps track of throughput and number of samples
type metadata struct {
	throughput int64
	samples    int64
}

type AdaptiveSampler struct {
	samplers            map[string]*ProbabilisticSampler
	upperbound          map[string]utils.RateLimiter
	targetQPS           float64
	probabilities       map[string]float64
	samples             map[string]int64
	lastTick            time.Time
	mux                 sync.Mutex
	calculationInterval time.Duration

	totalSampled map[string]*metadata
	start        time.Time
}

func NewAdaptiveSamplerV2(targetQPS float64) (*AdaptiveSampler, error) {
	sampler := &AdaptiveSampler{
		samples:             make(map[string]int64),
		samplers:            make(map[string]*ProbabilisticSampler),
		upperbound:          make(map[string]utils.RateLimiter),
		probabilities:       make(map[string]float64),
		targetQPS:           targetQPS,
		calculationInterval: time.Duration(1000000.0/targetQPS) * time.Microsecond * 2, // Nyquist rate
		start:               time.Now(),
		totalSampled:        make(map[string]*metadata),
	}
	go sampler.run()
	go sampler.log()
	return sampler, nil
}

func (a *AdaptiveSampler) run() {
	a.lastTick = time.Now()
	ticker := time.NewTicker(a.calculationInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.calculate()
		//case <-t.close:
		//	return
		}
	}
}

func (a *AdaptiveSampler) log() {
	ticker := time.NewTicker(time.Second * 60)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.mux.Lock()
			print(fmt.Sprintf("duration: %v\n", time.Now().Sub(a.start)))
			for operation, d := range a.totalSampled {
				print(fmt.Sprintf("operation: %s throughput: %v samples: %v\n", operation, d.throughput, d.samples))
			}
			a.mux.Unlock()
		}
	}
}

func (a *AdaptiveSampler) IsSampled(id TraceID, operation string) (bool, []Tag) {
	a.mux.Lock()
	defer a.mux.Unlock()
	if _, ok := a.samples[operation]; !ok {
		a.samples[operation] = 0
	}
	if _, ok := a.totalSampled[operation]; !ok {
		a.totalSampled[operation] = &metadata{}
	}
	var sampler *ProbabilisticSampler
	var ok bool
	if sampler, ok = a.samplers[operation]; !ok {
		sampler, _ = NewProbabilisticSampler(defaultProbability)
		a.probabilities[operation] = defaultProbability
		a.samplers[operation] = sampler

		upperbound := utils.NewRateLimiter(a.targetQPS, math.Max(a.targetQPS, 1.0))
		a.upperbound[operation] = upperbound
	}
	sampled, tags := sampler.IsSampled(id, operation)
	a.totalSampled[operation].throughput += 1
	if sampled {
		a.totalSampled[operation].samples += 1
		a.samples[operation] += 1
		upperbound := a.upperbound[operation]
		if !upperbound.CheckCredit(1.0) {
			// Can probably do better maths, ie throughput was X in this current window vs throughput for global window (if exists)
			newProb := a.probabilities[operation] / 2.0
			sampler, _ = NewProbabilisticSampler(newProb)
			a.samplers[operation] = sampler
			print("HALVING PROBABILITY")
		}
	}
	return sampled, tags
}

func (a *AdaptiveSampler) calculate() {
	a.mux.Lock()
	defer a.mux.Unlock()

	now := time.Now()
	interval := float64(now.Sub(a.lastTick)) / float64(time.Second)

	// Iterate over samplers since a operation may not have been sampled recently
	for operation, _ := range a.samplers {
		factor := 2.0
		if samples, ok := a.samples[operation]; ok && samples != 0 {
			observed := float64(samples) / interval
			factor = a.targetQPS / observed
		}
		// TODO if factor > 1, dampen

		a.probabilities[operation] *= factor
		a.samplers[operation] = newProbabilisticSampler(a.probabilities[operation])
	}
	a.lastTick = now
	a.samples = make(map[string]int64)
}

func (a *AdaptiveSampler) Close() {}

func (a *AdaptiveSampler) Equal(other Sampler) bool {
	return false
}

// TODO TTL on old operations, add limit on number of ops
