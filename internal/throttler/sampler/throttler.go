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

package sampler

import (
	"io"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/internal/throttler"
)

type ThrottledSampler struct {
	throttler throttler.Throttler
	sampler   jaeger.Sampler
}

func (t *ThrottledSampler) IsSampled(id jaeger.TraceID, operation string) (bool, []jaeger.Tag) {
	sampled, tags := t.sampler.IsSampled(id, operation)
	if sampled {
		if t.throttler.IsAllowed(operation) {
			return sampled, tags
		}
		// TODO if we keep doing this, we should reduce the sampling rate...
		return false, tags
	}
	return sampled, tags
}

func (t *ThrottledSampler) Close() {
	if throttler, ok := t.throttler.(io.Closer); ok {
		throttler.Close()
	}
	t.sampler.Close()
}

func (t *ThrottledSampler) Equal(other jaeger.Sampler) bool {
	return false
}
