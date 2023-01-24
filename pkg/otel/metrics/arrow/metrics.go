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
	"go.opentelemetry.io/collector/pdata/pmetric"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// Schema is the Arrow schema for the OTLP Arrow Metrics record.
var (
	Schema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ResourceMetrics, Type: arrow.ListOf(ResourceMetricsDT)},
	}, nil)
)

// MetricsBuilder is a helper to build a list of resource metrics.
type MetricsBuilder struct {
	released bool

	schema  *acommon.AdaptiveSchema // Metrics schema
	builder *array.RecordBuilder    // Record builder
	rmb     *array.ListBuilder      // resource metrics list builder
	rmp     *ResourceMetricsBuilder // resource metrics builder
}

// NewMetricsBuilder creates a new MetricsBuilder with a given allocator.
func NewMetricsBuilder(schema *acommon.AdaptiveSchema) (*MetricsBuilder, error) {
	builder := schema.RecordBuilder()
	rmb, ok := builder.Field(0).(*array.ListBuilder)
	if !ok {
		return nil, fmt.Errorf("expected field 0 to be a list builder, got %T", builder.Field(0))
	}
	return &MetricsBuilder{
		released: false,
		schema:   schema,
		builder:  builder,
		rmb:      rmb,
		rmp:      ResourceMetricsBuilderFrom(rmb.ValueBuilder().(*array.StructBuilder)),
	}, nil
}

// Build builds an Arrow Record from the builder.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the record.
func (b *MetricsBuilder) Build() (arrow.Record, error) {
	if b.released {
		return nil, fmt.Errorf("resource metrics builder already released")
	}

	defer b.Release()

	record := b.builder.NewRecord()

	overflowDetected, updates := b.schema.Analyze(record)
	if overflowDetected {
		record.Release()

		// Build a list of fields that overflowed
		var fieldNames []string
		for _, update := range updates {
			fieldNames = append(fieldNames, update.DictPath)
		}

		b.schema.UpdateSchema(updates)

		return nil, &acommon.DictionaryOverflowError{FieldNames: fieldNames}
	}

	return record, nil
}

// Append appends a new set of resource metrics to the builder.
func (b *MetricsBuilder) Append(metrics pmetric.Metrics) error {
	if b.released {
		return fmt.Errorf("metrics builder already released")
	}

	rm := metrics.ResourceMetrics()
	rc := rm.Len()
	if rc > 0 {
		b.rmb.Append(true)
		b.builder.Reserve(rc)
		for i := 0; i < rc; i++ {
			if err := b.rmp.Append(rm.At(i)); err != nil {
				return err
			}
		}
	} else {
		b.rmb.AppendNull()
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *MetricsBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
