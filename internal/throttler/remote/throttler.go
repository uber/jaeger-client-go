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

package remote

import (
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/uber-go/atomic"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/utils"
)

const (
	// minimumCredits is the minimum amount of credits necessary to not be throttled.
	// i.e. if currentCredits > minimumCredits, then the operation will not be throttled.
	minimumCredits = 1.0
)

type creditResponse struct {
	Operation string  `json:"operation"`
	Credits   float64 `json:"credits"`
}

type httpCreditManagerProxy struct {
	hostPort string
	logger   jaeger.Logger
}

func newHTTPCreditManagerProxy(hostPort string, logger jaeger.Logger) *httpCreditManagerProxy {
	return &httpCreditManagerProxy{
		hostPort: hostPort,
		logger:   logger,
	}
}

func (m *httpCreditManagerProxy) FetchCredits(uuid, serviceName string, operation []string) []creditResponse {
	params := url.Values{}
	params.Set("service", serviceName)
	params.Set("uuid", uuid)
	for _, op := range operation {
		params.Add("operation", op)
	}
	var resp []creditResponse
	if err := utils.GetJSON(fmt.Sprintf("http://%s/credits?%s", m.hostPort, params.Encode()), &resp); err != nil {
		m.logger.Error("Failed to receive credits from agent: " + err.Error())
	}
	return resp
}

// Throttler retrieves credits from agent and uses it to throttle operations.
type Throttler struct {
	options

	mux           sync.RWMutex
	service       string
	uuid          *atomic.String
	creditManager *httpCreditManagerProxy
	credits       map[string]float64 // map of operation->credits
	close         chan struct{}
	stopped       sync.WaitGroup
}

// NewThrottler returns a Throttler that polls agent for credits and uses them to throttle
// the service.
func NewThrottler(service string, options ...Option) *Throttler {
	// TODO add metrics
	// TODO set a limit on the max number of credits
	opts := applyOptions(options...)
	creditManager := newHTTPCreditManagerProxy(opts.hostPort, opts.logger)
	t := &Throttler{
		options:       opts,
		creditManager: creditManager,
		service:       service,
		credits:       make(map[string]float64),
		close:         make(chan struct{}),
		uuid:          atomic.NewString(""),
	}
	t.stopped.Add(1)
	go t.pollManager()
	return t
}

// IsThrottled implements Throttler#IsThrottled.
func (t *Throttler) IsThrottled(operation string) bool {
	t.mux.Lock()
	defer t.mux.Unlock()
	_, ok := t.credits[operation]
	if !ok {
		// If it is the first time this operation is being checked, synchronously fetch
		// the credits.
		credits := t.fetchCreditsHelper([]string{operation})
		if len(credits) == 0 {
			// Failed to receive credits from agent, try again next time
			return true
		}
		t.credits[operation] = credits[0].Credits
	}
	return t.isThrottled(operation)
}

// Close implements Throttler#Close.
func (t *Throttler) Close() error {
	close(t.close)
	t.stopped.Wait()
	return nil
}

// SetUUID implements Throttler#SetUUID. It's imperative that the UUID is set before any remote
// requests are made.
func (t *Throttler) SetUUID(uuid string) {
	t.uuid.Store(uuid)
}

// N.B. This function should be called with the Write Lock
func (t *Throttler) isThrottled(operation string) bool {
	credits := t.credits[operation]
	if credits < minimumCredits {
		return true
	}
	t.credits[operation] = credits - minimumCredits
	return false
}

func (t *Throttler) pollManager() {
	defer t.stopped.Done()
	ticker := time.NewTicker(t.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.fetchCredits()
		case <-t.close:
			return
		}
	}
}

func (t *Throttler) fetchCredits() {
	t.mux.RLock()
	// TODO This is probably inefficient, maybe create a static slice of operations?
	operations := make([]string, 0, len(t.credits))
	for op := range t.credits {
		operations = append(operations, op)
	}
	t.mux.RUnlock()
	newCredits := t.fetchCreditsHelper(operations)

	t.mux.Lock()
	defer t.mux.Unlock()
	for _, credit := range newCredits {
		t.credits[credit.Operation] += credit.Credits
	}
}

func (t *Throttler) fetchCreditsHelper(operations []string) []creditResponse {
	uuid := t.uuid.Load()
	if uuid == "" {
		t.logger.Error("Throttler uuid is not set, failed to fetch credits")
		return nil
	}
	return t.creditManager.FetchCredits(uuid, t.service, operations)
}
