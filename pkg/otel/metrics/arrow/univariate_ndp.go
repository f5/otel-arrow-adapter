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
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pmetric"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

var (
	// UnivariateNumberDataPointDT is the data type for a single univariate number data point.
	UnivariateNumberDataPointDT = arrow.StructOf(
		arrow.Field{Name: constants.Attributes, Type: acommon.AttributesDT},
		arrow.Field{Name: constants.StartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		arrow.Field{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		arrow.Field{Name: constants.MetricValue, Type: MetricValueDT},
		arrow.Field{Name: constants.Exemplars, Type: arrow.ListOf(ExemplarDT)},
		arrow.Field{Name: constants.Flags, Type: arrow.PrimitiveTypes.Uint32},
	)
)

// NumberDataPointBuilder is a builder for number data points.
type NumberDataPointBuilder struct {
	released bool

	builder *array.StructBuilder

	ab    *acommon.AttributesBuilder // attributes builder
	stunb *array.TimestampBuilder    // start_time_unix_nano builder
	tunb  *array.TimestampBuilder    // time_unix_nano builder
	mvb   *MetricValueBuilder        // metric_value builder
	elb   *array.ListBuilder         // exemplars builder
	eb    *ExemplarBuilder           // exemplar builder
	fb    *array.Uint32Builder       // flags builder
}

// NewNumberDataPointBuilder creates a new NumberDataPointBuilder with a given memory allocator.
func NewNumberDataPointBuilder(pool memory.Allocator) *NumberDataPointBuilder {
	return NumberDataPointBuilderFrom(array.NewStructBuilder(pool, UnivariateNumberDataPointDT))
}

// NumberDataPointBuilderFrom creates a new NumberDataPointBuilder from an existing StructBuilder.
func NumberDataPointBuilderFrom(ndpb *array.StructBuilder) *NumberDataPointBuilder {
	return &NumberDataPointBuilder{
		released: false,
		builder:  ndpb,

		ab:    acommon.AttributesBuilderFrom(ndpb.FieldBuilder(0).(*array.MapBuilder)),
		stunb: ndpb.FieldBuilder(1).(*array.TimestampBuilder),
		tunb:  ndpb.FieldBuilder(2).(*array.TimestampBuilder),
		mvb:   MetricValueBuilderFrom(ndpb.FieldBuilder(3).(*array.SparseUnionBuilder)),
		elb:   ndpb.FieldBuilder(4).(*array.ListBuilder),
		eb:    ExemplarBuilderFrom(ndpb.FieldBuilder(4).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
		fb:    ndpb.FieldBuilder(5).(*array.Uint32Builder),
	}
}

// Build builds the underlying array.
//
// Once the array is no longer needed, Release() should be called to free the memory.
func (b *NumberDataPointBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("QuantileValueBuilder: Build() called after Release()")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the underlying memory.
func (b *NumberDataPointBuilder) Release() {
	if b.released {
		return
	}

	b.released = true
	b.builder.Release()
}

// Append appends a new data point to the builder.
func (b *NumberDataPointBuilder) Append(ndp pmetric.NumberDataPoint, smdata *ScopeMetricsSharedData, mdata *MetricSharedData) error {
	if b.released {
		return fmt.Errorf("QuantileValueBuilder: AppendNItems() called after Release()")
	}

	b.builder.Append(true)
	if err := b.ab.AppendUniqueAttributes(ndp.Attributes(), smdata.Attributes, mdata.Attributes); err != nil {
		return err
	}
	if smdata.StartTime == nil && mdata.StartTime == nil {
		b.stunb.Append(arrow.Timestamp(ndp.StartTimestamp()))
	} else {
		b.stunb.AppendNull()
	}
	if smdata.Time == nil && mdata.Time == nil {
		b.tunb.Append(arrow.Timestamp(ndp.Timestamp()))
	} else {
		b.tunb.AppendNull()
	}
	if err := b.mvb.AppendNumberDataPointValue(ndp); err != nil {
		return err
	}
	exs := ndp.Exemplars()
	ec := exs.Len()
	if ec > 0 {
		b.elb.Append(true)
		b.elb.Reserve(ec)
		for i := 0; i < ec; i++ {
			if err := b.eb.Append(exs.At(i)); err != nil {
				return err
			}
		}
	} else {
		b.elb.Append(false)
	}
	b.fb.Append(uint32(ndp.Flags()))
	return nil
}
