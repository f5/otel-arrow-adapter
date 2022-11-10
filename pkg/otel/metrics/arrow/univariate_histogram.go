package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

var (
	// UnivariateHistogramDT is the Arrow Data Type describing a univariate histogram.
	UnivariateHistogramDT = arrow.StructOf(
		arrow.Field{Name: constants.DATA_POINTS, Type: arrow.ListOf(UnivariateHistogramDataPointDT)},
		arrow.Field{Name: constants.AGGREGATION_TEMPORALITY, Type: arrow.PrimitiveTypes.Int32},
	)
)

// UnivariateHistogramBuilder is a builder for histogram metrics.
type UnivariateHistogramBuilder struct {
	released bool

	builder *array.StructBuilder

	hdplb *array.ListBuilder         // data_points builder
	hdpb  *HistogramDataPointBuilder // histogram data point builder
	atb   *array.Int32Builder        // aggregation_temporality builder
}

// NewUnivariateHistogramBuilder creates a new UnivariateHistogramBuilder with a given memory allocator.
func NewUnivariateHistogramBuilder(pool memory.Allocator) *UnivariateHistogramBuilder {
	return UnivariateHistogramBuilderFrom(array.NewStructBuilder(pool, UnivariateHistogramDT))
}

// UnivariateHistogramBuilderFrom creates a new UnivariateHistogramBuilder from an existing StructBuilder.
func UnivariateHistogramBuilderFrom(arr *array.StructBuilder) *UnivariateHistogramBuilder {
	return &UnivariateHistogramBuilder{
		released: false,
		builder:  arr,

		hdplb: arr.FieldBuilder(0).(*array.ListBuilder),
		hdpb:  HistogramDataPointBuilderFrom(arr.FieldBuilder(0).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
		atb:   arr.FieldBuilder(1).(*array.Int32Builder),
	}
}

// Build builds the underlying array.
//
// Once the array is no longer needed, Release() should be called to free the memory.
func (b *UnivariateHistogramBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("UnivariateHistogramBuilder: Build() called after Release()")
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
func (b *UnivariateHistogramBuilder) Append(histogram pmetric.Histogram) error {
	if b.released {
		return fmt.Errorf("UnivariateHistogramBuilder: Append() called after Release()")
	}

	b.builder.Append(true)
	dps := histogram.DataPoints()
	dpc := dps.Len()
	if dpc > 0 {
		b.hdplb.Append(true)
		b.hdplb.Reserve(dpc)
		for i := 0; i < dpc; i++ {
			if err := b.hdpb.Append(dps.At(i)); err != nil {
				return err
			}
		}
	} else {
		b.hdplb.Append(false)
	}

	return nil
}
