package endtoend

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

var (
	testOperation = "testOperation"
	testService   = "testService"

	testConfig = config.Configuration{
		Disabled: false,
		Sampler: &config.SamplerConfig{
			Type:               jaeger.SamplerTypeRemote,
			Param:              1.0,
			LocalAgentHostPort: "localhost:5778",
		},
		Reporter: &config.ReporterConfig{
			BufferFlushInterval: time.Second,
			LocalAgentHostPort:  "localhost:5775",
		},
	}

	badConfig = config.Configuration{
		Disabled: false,
		Sampler: &config.SamplerConfig{
			Type: "INVALID_TYPE",
		},
	}

	testTraceRequest = traceRequest{
		Operation: testOperation,
		Tags: map[string]string{
			"key": "value",
		},
		Count: 2,
	}

	testInvalidJSON = `bad_json`

	testTraceJSONRequest = `
		{
			"operation": "testOperation",
			"tags": {
				"key": "value"
			},
			"count": 2
		}
	`
)

func newInMemoryTracer() (opentracing.Tracer, *jaeger.InMemoryReporter) {
	inMemoryReporter := jaeger.NewInMemoryReporter()
	tracer, _ := jaeger.NewTracer(
		testService,
		jaeger.NewConstSampler(true),
		inMemoryReporter,
		jaeger.TracerOptions.Metrics(jaeger.NewMetrics(jaeger.NullStatsReporter, nil)),
		jaeger.TracerOptions.Logger(jaeger.NullLogger))
	return tracer, inMemoryReporter
}

func TestInit(t *testing.T) {
	handler := &Handler{}
	err := handler.Init(testConfig)
	assert.NoError(t, err)
}

func TestInitBadConfig(t *testing.T) {
	handler := &Handler{}
	err := handler.Init(badConfig)
	assert.Error(t, err)
}

func TestTrace(t *testing.T) {
	tracer, _ := newInMemoryTracer()

	tests := []struct {
		expectedCode int
		json         string
		handler      *Handler
	}{
		{http.StatusOK, testTraceJSONRequest, &Handler{tracer: tracer}},
		{http.StatusBadRequest, testInvalidJSON, &Handler{}},
		{http.StatusBadRequest, testTraceJSONRequest, &Handler{}}, // Tracer hasn't been initialized
	}

	for _, test := range tests {
		req, err := http.NewRequest("POST", "/endtoend", bytes.NewBuffer([]byte(test.json)))
		if err != nil {
			assert.FailNow(t, "Failed to initialize request: %v", err)
		}
		recorder := httptest.NewRecorder()
		handlerFunc := http.HandlerFunc(test.handler.Trace)

		handlerFunc.ServeHTTP(recorder, req)

		assert.Equal(t, test.expectedCode, recorder.Code)
	}
}

func TestGenerateTraces(t *testing.T) {
	tracer, reporter := newInMemoryTracer()
	handler := &Handler{tracer: tracer}
	handler.generateTraces(&testTraceRequest)
	assert.Equal(t, 2, reporter.SpansSubmitted())
}
