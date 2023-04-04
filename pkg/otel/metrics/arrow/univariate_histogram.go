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

package arrow

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"go.opentelemetry.io/collector/pdata/pmetric"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

var (
	// UnivariateHistogramDT is the Arrow Data Type describing a univariate histogram.
	UnivariateHistogramDT = arrow.StructOf(
		arrow.Field{Name: constants.DataPoints, Type: arrow.ListOf(UnivariateHistogramDataPointDT), Metadata: schema.Metadata(schema.Optional)},
		arrow.Field{Name: constants.AggregationTemporality, Type: arrow.PrimitiveTypes.Int32, Metadata: schema.Metadata(schema.Optional, schema.Dictionary)},
	)
)

// UnivariateHistogramBuilder is a builder for histogram metrics.
type UnivariateHistogramBuilder struct {
	released bool

	builder *builder.StructBuilder

	hdplb *builder.ListBuilder       // data_points builder
	hdpb  *HistogramDataPointBuilder // histogram data point builder
	atb   *builder.Int32Builder      // aggregation_temporality builder
}

// UnivariateHistogramBuilderFrom creates a new UnivariateHistogramBuilder from an existing StructBuilder.
func UnivariateHistogramBuilderFrom(b *builder.StructBuilder) *UnivariateHistogramBuilder {
	hdplb := b.ListBuilder(constants.DataPoints)

	return &UnivariateHistogramBuilder{
		released: false,
		builder:  b,

		hdplb: hdplb,
		hdpb:  HistogramDataPointBuilderFrom(hdplb.StructBuilder()),
		atb:   b.Int32Builder(constants.AggregationTemporality),
	}
}

// Build builds the underlying array.
//
// Once the array is no longer needed, Release() should be called to free the memory.
func (b *UnivariateHistogramBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the underlying memory used by the builder.
func (b *UnivariateHistogramBuilder) Release() {
	if b.released {
		return
	}

	b.released = true
	b.builder.Release()
}

// Append appends a new histogram to the builder.
func (b *UnivariateHistogramBuilder) Append(histogram pmetric.Histogram, smdata *ScopeMetricsSharedData, mdata *MetricSharedData) error {
	if b.released {
		return werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	return b.builder.Append(histogram, func() error {
		dps := histogram.DataPoints()
		dpc := dps.Len()
		if err := b.hdplb.Append(dpc, func() error {
			for i := 0; i < dpc; i++ {
				if err := b.hdpb.Append(dps.At(i), smdata, mdata); err != nil {
					return werror.Wrap(err)
				}
			}
			return nil
		}); err != nil {
			return werror.Wrap(err)
		}
		if histogram.AggregationTemporality() == pmetric.AggregationTemporalityUnspecified {
			b.atb.AppendNull()
		} else {
			b.atb.AppendNonZero(int32(histogram.AggregationTemporality()))
		}

		return nil
	})
}

func (b *UnivariateHistogramBuilder) AppendNull() {
	if b.released {
		return
	}

	b.builder.AppendNull()
}
