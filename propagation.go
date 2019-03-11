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

package jaeger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
)

var (
	// errCannotMergeSpanContexts occurs when a CompositePropagator is unable to merge multiple SpanContexts extracted
	// via the underlying Extractors due to clashes. Note that this is only used internally by the composite propagator.
	errCannotMergeSpanContexts = errors.New("Composite propagator cannot merge extracted span contexts")
)

// Injector is responsible for injecting SpanContext instances in a manner suitable
// for propagation via a format-specific "carrier" object. Typically the
// injection will take place across an RPC boundary, but message queues and
// other IPC mechanisms are also reasonable places to use an Injector.
type Injector interface {
	// Inject takes `SpanContext` and injects it into `carrier`. The actual type
	// of `carrier` depends on the `format` passed to `Tracer.Inject()`.
	//
	// Implementations may return opentracing.ErrInvalidCarrier or any other
	// implementation-specific error if injection fails.
	Inject(ctx SpanContext, carrier interface{}) error
}

// Extractor is responsible for extracting SpanContext instances from a
// format-specific "carrier" object. Typically the extraction will take place
// on the server side of an RPC boundary, but message queues and other IPC
// mechanisms are also reasonable places to use an Extractor.
type Extractor interface {
	// Extract decodes a SpanContext instance from the given `carrier`,
	// or (nil, opentracing.ErrSpanContextNotFound) if no context could
	// be found in the `carrier`.
	Extract(carrier interface{}) (SpanContext, error)
}

// Propagator is a composite type embedding both an Injector and an Extractor.
type Propagator interface {
	Injector
	Extractor
}

// TextMapPropagator can be used to extract and inject Jaeger headers into SpanContexts using the TextMap format.
type TextMapPropagator struct {
	headerKeys  *HeadersConfig
	metrics     Metrics
	encodeValue func(string) string
	decodeValue func(string) string
}

// NewTextMapPropagator creates a Propagator for extracting and injecting Jaeger headers into SpanContexts using the
// TextMap format.
func NewTextMapPropagator(headerKeys *HeadersConfig, metrics Metrics) *TextMapPropagator {
	return &TextMapPropagator{
		headerKeys: headerKeys,
		metrics:    metrics,
		encodeValue: func(val string) string {
			return val
		},
		decodeValue: func(val string) string {
			return val
		},
	}
}

// NewHTTPHeaderPropagator creates a Propagator for extracting and injecting Jaeger headers into SpanContexts using the
// HTTP format.
func NewHTTPHeaderPropagator(headerKeys *HeadersConfig, metrics Metrics) *TextMapPropagator {
	return &TextMapPropagator{
		headerKeys: headerKeys,
		metrics:    metrics,
		encodeValue: func(val string) string {
			return url.QueryEscape(val)
		},
		decodeValue: func(val string) string {
			// ignore decoding errors, cannot do anything about them
			if v, err := url.QueryUnescape(val); err == nil {
				return v
			}
			return val
		},
	}
}

// BinaryPropagator can be used to extract and inject Jaeger headers into SpanContexts using the Binary format.
type BinaryPropagator struct {
	tracer  *Tracer
	buffers sync.Pool
}

// NewBinaryPropagator creates a Propagator for extracting and injecting Jaeger headers into SpanContexts using the
// Binary format.
func NewBinaryPropagator(tracer *Tracer) *BinaryPropagator {
	return &BinaryPropagator{
		tracer:  tracer,
		buffers: sync.Pool{New: func() interface{} { return &bytes.Buffer{} }},
	}
}

// Inject conforms to the Injector interface for decoding Jaeger headers using the default TextMap format
func (p *TextMapPropagator) Inject(
	sc SpanContext,
	abstractCarrier interface{},
) error {
	textMapWriter, ok := abstractCarrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}

	// Do not encode the string with trace context to avoid accidental double-encoding
	// if people are using opentracing < 0.10.0. Our colon-separated representation
	// of the trace context is already safe for HTTP headers.
	textMapWriter.Set(p.headerKeys.TraceContextHeaderName, sc.String())
	for k, v := range sc.baggage {
		safeKey := p.addBaggageKeyPrefix(k)
		safeVal := p.encodeValue(v)
		textMapWriter.Set(safeKey, safeVal)
	}
	return nil
}

// Extract conforms to the Extractor interface for injecting Jaeger headers using the default TextMap format
func (p *TextMapPropagator) Extract(abstractCarrier interface{}) (SpanContext, error) {
	textMapReader, ok := abstractCarrier.(opentracing.TextMapReader)
	if !ok {
		return emptyContext, opentracing.ErrInvalidCarrier
	}
	var ctx SpanContext
	var baggage map[string]string
	err := textMapReader.ForeachKey(func(rawKey, value string) error {
		key := strings.ToLower(rawKey) // TODO not necessary for plain TextMap
		if key == p.headerKeys.TraceContextHeaderName {
			var err error
			safeVal := p.decodeValue(value)
			if ctx, err = ContextFromString(safeVal); err != nil {
				return err
			}
		} else if key == p.headerKeys.JaegerDebugHeader {
			ctx.debugID = p.decodeValue(value)
		} else if key == p.headerKeys.JaegerBaggageHeader {
			if baggage == nil {
				baggage = make(map[string]string)
			}
			for k, v := range p.parseCommaSeparatedMap(value) {
				baggage[k] = v
			}
		} else if strings.HasPrefix(key, p.headerKeys.TraceBaggageHeaderPrefix) {
			if baggage == nil {
				baggage = make(map[string]string)
			}
			safeKey := p.removeBaggageKeyPrefix(key)
			safeVal := p.decodeValue(value)
			baggage[safeKey] = safeVal
		}
		return nil
	})
	if err != nil {
		p.metrics.DecodingErrors.Inc(1)
		return emptyContext, err
	}
	if !ctx.traceID.IsValid() && ctx.debugID == "" && len(baggage) == 0 {
		return emptyContext, opentracing.ErrSpanContextNotFound
	}
	ctx.baggage = baggage
	return ctx, nil
}

// Inject conforms to the Injector interface for decoding Jaeger headers using the Binary format
func (p *BinaryPropagator) Inject(
	sc SpanContext,
	abstractCarrier interface{},
) error {
	carrier, ok := abstractCarrier.(io.Writer)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}

	// Handle the tracer context
	if err := binary.Write(carrier, binary.BigEndian, sc.traceID); err != nil {
		return err
	}
	if err := binary.Write(carrier, binary.BigEndian, sc.spanID); err != nil {
		return err
	}
	if err := binary.Write(carrier, binary.BigEndian, sc.parentID); err != nil {
		return err
	}
	if err := binary.Write(carrier, binary.BigEndian, sc.flags); err != nil {
		return err
	}

	// Handle the baggage items
	if err := binary.Write(carrier, binary.BigEndian, int32(len(sc.baggage))); err != nil {
		return err
	}
	for k, v := range sc.baggage {
		if err := binary.Write(carrier, binary.BigEndian, int32(len(k))); err != nil {
			return err
		}
		io.WriteString(carrier, k)
		if err := binary.Write(carrier, binary.BigEndian, int32(len(v))); err != nil {
			return err
		}
		io.WriteString(carrier, v)
	}

	return nil
}

// Extract conforms to the Extractor interface for injecting Jaeger headers using the Binary format
func (p *BinaryPropagator) Extract(abstractCarrier interface{}) (SpanContext, error) {
	carrier, ok := abstractCarrier.(io.Reader)
	if !ok {
		return emptyContext, opentracing.ErrInvalidCarrier
	}
	var ctx SpanContext

	if err := binary.Read(carrier, binary.BigEndian, &ctx.traceID); err != nil {
		return emptyContext, opentracing.ErrSpanContextCorrupted
	}
	if err := binary.Read(carrier, binary.BigEndian, &ctx.spanID); err != nil {
		return emptyContext, opentracing.ErrSpanContextCorrupted
	}
	if err := binary.Read(carrier, binary.BigEndian, &ctx.parentID); err != nil {
		return emptyContext, opentracing.ErrSpanContextCorrupted
	}
	if err := binary.Read(carrier, binary.BigEndian, &ctx.flags); err != nil {
		return emptyContext, opentracing.ErrSpanContextCorrupted
	}

	// Handle the baggage items
	var numBaggage int32
	if err := binary.Read(carrier, binary.BigEndian, &numBaggage); err != nil {
		return emptyContext, opentracing.ErrSpanContextCorrupted
	}
	if iNumBaggage := int(numBaggage); iNumBaggage > 0 {
		ctx.baggage = make(map[string]string, iNumBaggage)
		buf := p.buffers.Get().(*bytes.Buffer)
		defer p.buffers.Put(buf)

		var keyLen, valLen int32
		for i := 0; i < iNumBaggage; i++ {
			if err := binary.Read(carrier, binary.BigEndian, &keyLen); err != nil {
				return emptyContext, opentracing.ErrSpanContextCorrupted
			}
			buf.Reset()
			buf.Grow(int(keyLen))
			if n, err := io.CopyN(buf, carrier, int64(keyLen)); err != nil || int32(n) != keyLen {
				return emptyContext, opentracing.ErrSpanContextCorrupted
			}
			key := buf.String()

			if err := binary.Read(carrier, binary.BigEndian, &valLen); err != nil {
				return emptyContext, opentracing.ErrSpanContextCorrupted
			}
			buf.Reset()
			buf.Grow(int(valLen))
			if n, err := io.CopyN(buf, carrier, int64(valLen)); err != nil || int32(n) != valLen {
				return emptyContext, opentracing.ErrSpanContextCorrupted
			}
			ctx.baggage[key] = buf.String()
		}
	}

	return ctx, nil
}

// Converts a comma separated key value pair list into a map
// e.g. key1=value1, key2=value2, key3 = value3
// is converted to map[string]string { "key1" : "value1",
//                                     "key2" : "value2",
//                                     "key3" : "value3" }
func (p *TextMapPropagator) parseCommaSeparatedMap(value string) map[string]string {
	baggage := make(map[string]string)
	value, err := url.QueryUnescape(value)
	if err != nil {
		log.Printf("Unable to unescape %s, %v", value, err)
		return baggage
	}
	for _, kvpair := range strings.Split(value, ",") {
		kv := strings.Split(strings.TrimSpace(kvpair), "=")
		if len(kv) == 2 {
			baggage[kv[0]] = kv[1]
		} else {
			log.Printf("Malformed value passed in for %s", p.headerKeys.JaegerBaggageHeader)
		}
	}
	return baggage
}

// Converts a baggage item key into an http header format,
// by prepending TraceBaggageHeaderPrefix and encoding the key string
func (p *TextMapPropagator) addBaggageKeyPrefix(key string) string {
	// TODO encodeBaggageKeyAsHeader add caching and escaping
	return fmt.Sprintf("%v%v", p.headerKeys.TraceBaggageHeaderPrefix, key)
}

func (p *TextMapPropagator) removeBaggageKeyPrefix(key string) string {
	// TODO decodeBaggageHeaderKey add caching and escaping
	return key[len(p.headerKeys.TraceBaggageHeaderPrefix):]
}

// CompositePropagator can be used to extract and inject OpenTracing headers into SpanContexts using multiple
// propagators.
type CompositePropagator struct {
	propagators []Propagator
}

// NewCompositePropagator creates a Propagator that can be used to perform extraction and injection of SpanContexts from
// and into carriers from multiple Propagators.
func NewCompositePropagator(propagators []Propagator) *CompositePropagator {
	return &CompositePropagator{
		propagators: propagators,
	}
}

// Inject attempts to inject a SpanContext into a given carrier using the underlying propagators. If one of the
// propagators is unable to inject the SpanContext, this method errors out (causing injection attempts using the
// subsequent propagators to be skipped).
func (p *CompositePropagator) Inject(sc SpanContext, abstractCarrier interface{}) error {
	for _, propagator := range p.propagators {
		err := propagator.Inject(sc, abstractCarrier)
		if err != nil {
			return err
		}
	}

	return nil
}

// Extract attempts to extract a SpanContext from the given carrier using the underlying propagators.
//
// If multiple propagators are capable of extracting a valid context, this propagator will ensure the contexts match
// eachother. Contexts are considered as non-clashing as long as their IDs and baggage match (or extend) eachother. If
// the extracted contexts clash (e.g. by having different values for what would be the same baggage item or different
// IDs), an ErrSpanContextCorrupted will be returned.
//
// If no underlying propagator is able to extract a valid span, all the errors will be combined together in order to
// produce a single (opentracing-compliant) error (see mergeExtractedError for specifics on this).
func (p *CompositePropagator) Extract(abstractCarrier interface{}) (SpanContext, error) {
	extractedContexts := make([]SpanContext, 0)
	extractedErrors := make([]error, 0)

	for _, propagator := range p.propagators {
		sc, err := propagator.Extract(abstractCarrier)

		if err != nil {
			extractedErrors = append(extractedErrors, err)
		} else {
			extractedContexts = append(extractedContexts, sc)
		}
	}

	return combineExtractResults(extractedContexts, extractedErrors)
}

func combineExtractResults(contexts []SpanContext, errors []error) (SpanContext, error) {
	ret := emptyContext
	var err error

	for _, sc := range contexts {
		// Base case
		if !ret.IsValid() && !ret.isDebugIDContainerOnly() {
			ret = sc
		} else if sc.IsValid() {
			// If the context is valid (i.e. a full span context), it should either be merged with another valid context
			// (that we've already extracted) or a debug container
			if ret.IsValid() {
				ret, err = mergeValidContexts(ret, sc)
			} else {
				ret, err = mergeContextWithDebugIDContainer(sc, ret)
			}

			if err != nil {
				return emptyContext, opentracing.ErrSpanContextCorrupted
			}
		} else if sc.isDebugIDContainerOnly() {
			// If the context represents a debug container, we can ignore the remaining fields and simply attempt to
			// extend what we already have (ensuring the debug info doesn't clash either)
			ret, err = mergeContextWithDebugIDContainer(ret, sc)

			if err != nil {
				return emptyContext, opentracing.ErrSpanContextCorrupted
			}
		}
	}

	// If we were unable to produce a usable span (i.e. valid or a debug container), we need to return an empty context
	// and combine all the errors into one
	if !ret.IsValid() && !ret.isDebugIDContainerOnly() {
		return emptyContext, mergeExtractedErrors(errors)
	}

	return ret, nil
}

func mergeContextWithDebugIDContainer(base SpanContext, new SpanContext) (SpanContext, error) {
	if base.debugID == "" {
		base.debugID = new.debugID
	} else if base.debugID != new.debugID {
		return emptyContext, errCannotMergeSpanContexts
	}

	return base, nil
}

func mergeValidContexts(base SpanContext, new SpanContext) (SpanContext, error) {
	var err error

	// Ensure the basic IDs don't clash
	if base.traceID != new.traceID ||
		base.spanID != new.spanID ||
		base.parentID != new.parentID {
		return emptyContext, errCannotMergeSpanContexts
	}

	// Merge (or extend) debug information
	if new.debugID != "" {
		base, err = mergeContextWithDebugIDContainer(base, new)

		if err != nil {
			return emptyContext, errCannotMergeSpanContexts
		}
	}

	// Extend the flags
	base.flags = base.flags | new.flags

	// Extend the baggage as long as there are no clashes
	for k, v := range new.baggage {
		if baseVal, ok := base.baggage[k]; ok && baseVal != v {
			return emptyContext, errCannotMergeSpanContexts
		}

		base.baggage[k] = v
	}

	return base, nil
}

// mergeExtractedErrors merges multiple Extractor.Extract() errors (obtained from an unsuccessful extraction by a
// composite propagator) into one.
//
// The merging logic is as follows:
// - If at least one Extractor was able to interpret data from the carrier (i.e. the carrier was valid and matched the
// expected format), we assume the client had the intention of propagating data in that format and using that Extractor.
// - If at least one Extractor went as far in the extraction process as reaching lower level errors (e.g.
// ErrSpanContextCorrupted or custom decoding errors when every other error was already checked for),
// ErrSpanContextCorrupted will be returned.
// - If no Extractor went that far, the "dirtiest" error - i.e. the error that is caused further down the error pipeline
// (e.g. an ErrSpanContextNotFound shouldn't even happen if the carrier is invalid, as ErrInvalidCarrier has precedence
// over it) - will be returned.
func mergeExtractedErrors(errors []error) error {
	ret := opentracing.ErrSpanContextNotFound

	for _, err := range errors {
		switch err {
		case opentracing.ErrSpanContextCorrupted:
			// Return immediately as this is the dirtiest possible error recognized by opentracing
			return err
		case opentracing.ErrInvalidCarrier:
			ret = err
		case opentracing.ErrInvalidSpanContext:
			if ret != opentracing.ErrInvalidCarrier {
				ret = err
			}
		case opentracing.ErrUnsupportedFormat:
			if ret != opentracing.ErrInvalidCarrier && ret != opentracing.ErrInvalidSpanContext {
				ret = err
			}
		case opentracing.ErrSpanContextNotFound:
			// Do nothing as this is the base error
		default:
			// Some other error leaked through (assume the span is corrupted)
			return opentracing.ErrSpanContextCorrupted
		}
	}

	return ret
}
