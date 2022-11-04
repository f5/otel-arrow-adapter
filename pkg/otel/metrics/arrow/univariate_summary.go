package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// UnivariateSummaryDT is the Arrow Data Type describing a univariate summary.
var (
	UnivariateSummaryDT = arrow.StructOf(
		arrow.Field{Name: constants.DATA_POINTS, Type: arrow.ListOf(QuantileValueDT)},
	)
)

// UnivariateSummaryBuilder is a builder for summary metrics.
type UnivariateSummaryBuilder struct {
	released bool

	builder *array.StructBuilder

	dplb *array.ListBuilder      // data_points builder
	dpb  *NumberDataPointBuilder // number data point builder
	atb  *array.Int32Builder     // aggregation_temporality builder
	imb  *array.BooleanBuilder   // is_monotonic builder
}

// NewUnivariateSummaryBuilder creates a new UnivariateSummaryBuilder with a given memory allocator.
func NewUnivariateSummaryBuilder(pool memory.Allocator) *UnivariateSummaryBuilder {
	return UnivariateSummaryBuilderFrom(array.NewStructBuilder(pool, UnivariateSummaryDT))
}

// UnivariateSummaryBuilderFrom creates a new UnivariateSummaryBuilder from an existing StructBuilder.
func UnivariateSummaryBuilderFrom(ndpb *array.StructBuilder) *UnivariateSummaryBuilder {
	return &UnivariateSummaryBuilder{
		released: false,
		builder:  ndpb,

		dplb: ndpb.FieldBuilder(0).(*array.ListBuilder),
		dpb:  NumberDataPointBuilderFrom(ndpb.FieldBuilder(0).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
		atb:  ndpb.FieldBuilder(1).(*array.Int32Builder),
		imb:  ndpb.FieldBuilder(2).(*array.BooleanBuilder),
	}
}

// Build builds the underlying array.
//
// Once the array is no longer needed, Release() should be called to free the memory.
func (b *UnivariateSummaryBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("UnivariateMetricBuilder: Build() called after Release()")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the underlying memory.
func (b *UnivariateSummaryBuilder) Release() {
	if b.released {
		return
	}

	b.released = true
	b.builder.Release()
}

// Append appends a new univariate summary to the builder.
func (b *UnivariateSummaryBuilder) Append(gauge pmetric.Summary) error {
	if b.released {
		return fmt.Errorf("UnivariateMetricBuilder: Append() called after Release()")
	}

	// TODO

	return nil
}
