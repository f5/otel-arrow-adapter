package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pmetric"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

var (
	// MetricSetDT is the Arrow Data Type describing a metric set.
	MetricSetDT = arrow.StructOf([]arrow.Field{
		{Name: constants.NAME, Type: acommon.DictU16String},
		{Name: constants.DESCRIPTION, Type: acommon.DictU16String},
		{Name: constants.UNIT, Type: acommon.DictU16String},
		// Data
		// - Attributes
		// - Start Time
		// - End Time
		// - Value
		// - Exemplars
		// - Flags
	}...)

	// MultivariateMetricsDT is the Arrow Data Type describing a multivariate metrics.
	MultivariateMetricsDT = arrow.StructOf([]arrow.Field{
		{Name: constants.NAME, Type: acommon.DictU16String},
		{Name: constants.DESCRIPTION, Type: acommon.DictU16String},
		{Name: constants.UNIT, Type: acommon.DictU16String},
		// attributes
		// start time
		// end time
		// [metrics]
		// - sub-name
		// - value
		// - exemplars
		// - flags
	}...)
)

// MetricSetBuilder is a helper to build a metric set.
type MetricSetBuilder struct {
	released bool

	builder *array.StructBuilder

	nb *array.BinaryDictionaryBuilder // metric name builder
	db *array.BinaryDictionaryBuilder // metric description builder
	ub *array.BinaryDictionaryBuilder // metric unit builder
}

// NewMetricSetBuilder creates a new SpansBuilder with a given allocator.
//
// Once the builder is no longer needed, Release() must be called to free the
// memory allocated by the builder.
func NewMetricSetBuilder(pool memory.Allocator) *MetricSetBuilder {
	sb := array.NewStructBuilder(pool, MetricSetDT)
	return MetricSetBuilderFrom(sb)
}

func MetricSetBuilderFrom(sb *array.StructBuilder) *MetricSetBuilder {
	return &MetricSetBuilder{
		released: false,
		builder:  sb,
		nb:       sb.FieldBuilder(0).(*array.BinaryDictionaryBuilder),
		db:       sb.FieldBuilder(1).(*array.BinaryDictionaryBuilder),
		ub:       sb.FieldBuilder(2).(*array.BinaryDictionaryBuilder),
	}
}

// Build builds the span array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *MetricSetBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("span builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Append appends a new metric to the builder.
func (b *MetricSetBuilder) Append(metric pmetric.Metric) error {
	if b.released {
		return fmt.Errorf("metric set builder already released")
	}

	b.builder.Append(true)

	name := metric.Name()
	if name == "" {
		b.nb.AppendNull()
	} else {
		if err := b.nb.AppendString(name); err != nil {
			return err
		}
	}
	desc := metric.Description()
	if desc == "" {
		b.db.AppendNull()
	} else {
		if err := b.db.AppendString(desc); err != nil {
			return err
		}
	}
	unit := metric.Unit()
	if unit == "" {
		b.ub.AppendNull()
	} else {
		if err := b.ub.AppendString(unit); err != nil {
			return err
		}
	}

	return nil
}

// Release releases the memory allocated by the builder.
func (b *MetricSetBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.nb.Release()
		b.db.Release()
		b.ub.Release()

		b.released = true
	}
}