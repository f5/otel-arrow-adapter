// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlp

import (
	otlp "github.com/f5/otel-arrow-adapter/pkg/otel/common/otlp"
)

type UnivariateHistogramDataPointIds struct {
	Id                int
	Attributes        *otlp.AttributeIds
	StartTimeUnixNano int
	TimeUnixNano      int
	Count             int
	Sum               int
	BucketCounts      int // List of uint64
	ExplicitBounds    int // List of float64
	Exemplars         *ExemplarIds
	Flags             int
	Min               int
	Max               int
}
