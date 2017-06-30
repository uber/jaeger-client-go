// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package testutils

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/uber/jaeger-client-go/thrift-gen/agent"
	"github.com/uber/jaeger-client-go/thrift-gen/baggage"
	"github.com/uber/jaeger-client-go/thrift-gen/jaeger"
	"github.com/uber/jaeger-client-go/thrift-gen/sampling"
	"github.com/uber/jaeger-client-go/thrift-gen/zipkincore"
	"github.com/uber/jaeger-client-go/utils"
)

// StartMockAgent runs a mock representation of jaeger-agent.
// This function returns a started server.
func StartMockAgent() (*MockAgent, error) {
	transport, err := NewTUDPServerTransport("127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	samplingManager := newSamplingManager()
	baggageManager := newBaggageRestrictionManager()
	agentHandler := &agentHandler{samplingManager: samplingManager, baggageRestrictionManager: baggageManager}
	mux := http.NewServeMux()
	mux.HandleFunc("/baggageRestrictions", agentHandler.getBaggageRestrictions)
	mux.HandleFunc("/", agentHandler.getSamplingStrategy)
	agentServer := httptest.NewServer(mux)

	agent := &MockAgent{
		transport:   transport,
		samplingMgr: samplingManager,
		baggageMgr:  baggageManager,
		server:      agentServer,
	}

	var started sync.WaitGroup
	started.Add(1)
	go agent.serve(&started)
	started.Wait()

	return agent, nil
}

// Close stops the serving of traffic
func (s *MockAgent) Close() {
	atomic.StoreUint32(&s.serving, 0)
	s.transport.Close()
	s.server.Close()
}

// MockAgent is a mock representation of Jaeger Agent.
// It receives spans over UDP, and has an HTTP endpoint for sampling strategies.
type MockAgent struct {
	transport     *TUDPTransport
	jaegerBatches []*jaeger.Batch
	mutex         sync.Mutex
	serving       uint32
	samplingMgr   *samplingManager
	baggageMgr    *baggageRestrictionManager
	server        *httptest.Server
}

// SpanServerAddr returns the UDP host:port where MockAgent listens for spans
func (s *MockAgent) SpanServerAddr() string {
	return s.transport.Addr().String()
}

// SpanServerClient returns a UDP client that can be used to send spans to the MockAgent
func (s *MockAgent) SpanServerClient() (agent.Agent, error) {
	return utils.NewAgentClientUDP(s.SpanServerAddr(), 0)
}

// ServerAddr returns the host:port of HTTP server exposing all agent endpoints
func (s *MockAgent) ServerAddr() string {
	return s.server.Listener.Addr().String()
}

func (s *MockAgent) serve(started *sync.WaitGroup) {
	handler := agent.NewAgentProcessor(s)
	protocolFact := thrift.NewTCompactProtocolFactory()
	buf := make([]byte, utils.UDPPacketMaxLength, utils.UDPPacketMaxLength)
	trans := thrift.NewTMemoryBufferLen(utils.UDPPacketMaxLength)

	atomic.StoreUint32(&s.serving, 1)
	started.Done()
	for s.IsServing() {
		n, err := s.transport.Read(buf)
		if err == nil {
			trans.Write(buf[:n])
			protocol := protocolFact.GetProtocol(trans)
			handler.Process(protocol, protocol)
		}
	}
}

// EmitZipkinBatch is deprecated, use EmitBatch
func (s *MockAgent) EmitZipkinBatch(spans []*zipkincore.Span) (err error) {
	// TODO remove this for 3.0.0
	return errors.New("Not implemented")
}

// GetZipkinSpans is deprecated use GetJaegerBatches
func (s *MockAgent) GetZipkinSpans() []*zipkincore.Span {
	return nil
}

// ResetZipkinSpans is deprecated use ResetJaegerBatches
func (s *MockAgent) ResetZipkinSpans() {}

// EmitBatch implements EmitBatch() of TChanSamplingManagerServer
func (s *MockAgent) EmitBatch(batch *jaeger.Batch) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.jaegerBatches = append(s.jaegerBatches, batch)
	return err
}

// IsServing indicates whether the server is currently serving traffic
func (s *MockAgent) IsServing() bool {
	return atomic.LoadUint32(&s.serving) == 1
}

// AddSamplingStrategy registers a sampling strategy for a service
func (s *MockAgent) AddSamplingStrategy(service string, strategy *sampling.SamplingStrategyResponse) {
	s.samplingMgr.AddSamplingStrategy(service, strategy)
}

// GetJaegerBatches returns accumulated Jaeger batches
func (s *MockAgent) GetJaegerBatches() []*jaeger.Batch {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	n := len(s.jaegerBatches)
	batches := make([]*jaeger.Batch, n, n)
	copy(batches, s.jaegerBatches)
	return batches
}

// AddBaggageRestrictions registers a baggage restriction for a service
func (s *MockAgent) AddBaggageRestrictions(service string, restrictions []*baggage.BaggageRestriction) {
	s.baggageMgr.AddBaggageRestrictions(service, restrictions)
}

// ResetJaegerBatches discards accumulated Jaeger batches
func (s *MockAgent) ResetJaegerBatches() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.jaegerBatches = nil
}

type agentHandler struct {
	samplingManager           *samplingManager
	baggageRestrictionManager *baggageRestrictionManager
}

func getService(r *http.Request) (string, error) {
	services := r.URL.Query()["service"]
	if len(services) == 0 {
		return "", errors.New("'service' parameter is empty")
	}
	if len(services) > 1 {
		return "", errors.New("'service' parameter must occur only once")
	}
	return services[0], nil
}

func sendResponse(w http.ResponseWriter, resp interface{}) {
	bytes, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "Cannot marshall Thrift to JSON", http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	if _, err := w.Write(bytes); err != nil {
		return
	}
}

func (h *agentHandler) getBaggageRestrictions(w http.ResponseWriter, r *http.Request) {
	service, err := getService(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, _ := h.baggageRestrictionManager.GetBaggageRestrictions(service)
	sendResponse(w, resp)
}

func (h *agentHandler) getSamplingStrategy(w http.ResponseWriter, r *http.Request) {
	service, err := getService(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, _ := h.samplingManager.GetSamplingStrategy(service)
	sendResponse(w, resp)
}
