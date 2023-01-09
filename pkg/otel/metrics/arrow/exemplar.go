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
	// ExemplarDT is an Arrow Data Type representing an OTLP metric exemplar.
	ExemplarDT = arrow.StructOf(
		arrow.Field{Name: constants.Attributes, Type: acommon.AttributesDT},
		arrow.Field{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		arrow.Field{Name: constants.MetricValue, Type: MetricValueDT},
		// TODO: Not sure a dictionary if needed here
		arrow.Field{Name: constants.SpanId, Type: acommon.DefaultDictFixed8Binary},
		arrow.Field{Name: constants.TraceId, Type: acommon.DefaultDictFixed16Binary},
	)
)

// ExemplarBuilder is a helper to build an Arrow array containing a collection of OTLP metric exemplar.
type ExemplarBuilder struct {
	released bool

	builder *array.StructBuilder // exemplar value builder

	ab   *acommon.AttributesBuilder         // attributes builder
	tunb *array.TimestampBuilder            // time unix nano builder
	mvb  *MetricValueBuilder                // metric value builder
	sib  *acommon.AdaptiveDictionaryBuilder // span id builder
	tib  *acommon.AdaptiveDictionaryBuilder // trace id builder
}

// NewExemplarBuilder creates a new ExemplarBuilder with a given memory allocator.
func NewExemplarBuilder(pool memory.Allocator) *ExemplarBuilder {
	return ExemplarBuilderFrom(array.NewStructBuilder(pool, ExemplarDT))
}

// ExemplarBuilderFrom creates a new ExemplarBuilder from an existing StructBuilder.
func ExemplarBuilderFrom(ex *array.StructBuilder) *ExemplarBuilder {
	return &ExemplarBuilder{
		released: false,
		builder:  ex,

		ab:   acommon.AttributesBuilderFrom(ex.FieldBuilder(0).(*array.MapBuilder)),
		tunb: ex.FieldBuilder(1).(*array.TimestampBuilder),
		mvb:  MetricValueBuilderFrom(ex.FieldBuilder(2).(*array.DenseUnionBuilder)),
		sib:  acommon.AdaptiveDictionaryBuilderFrom(ex.FieldBuilder(3)),
		tib:  acommon.AdaptiveDictionaryBuilderFrom(ex.FieldBuilder(4)),
	}
}

// Build builds the exemplar Arrow array.
//
// Once the returned array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ExemplarBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("exemplar builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Append appends an exemplar to the builder.
func (b *ExemplarBuilder) Append(ex pmetric.Exemplar) error {
	if b.released {
		return fmt.Errorf("exemplar builder already released")
	}

	b.builder.Append(true)
	if err := b.ab.Append(ex.FilteredAttributes()); err != nil {
		return err
	}
	b.tunb.Append(arrow.Timestamp(ex.Timestamp()))
	if err := b.mvb.AppendExemplarValue(ex); err != nil {
		return err
	}

	sid := ex.SpanID()
	if sid.IsEmpty() {
		b.sib.AppendNull()
	} else {
		if err := b.sib.AppendBinary(sid[:]); err != nil {
			return err
		}
	}

	tid := ex.TraceID()
	if tid.IsEmpty() {
		b.tib.AppendNull()
	} else {
		if err := b.tib.AppendBinary(tid[:]); err != nil {
			return err
		}
	}

	return nil
}

// Release releases the memory allocated by the builder.
func (b *ExemplarBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
