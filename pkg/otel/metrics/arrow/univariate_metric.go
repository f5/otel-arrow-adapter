package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// Constants used to identify the type of univariate metric in the union.
const (
	GaugeCode   int8 = 1
	SumCode     int8 = 2
	SummaryCode int8 = 3
	// HistogramCode int8 = 4
	// ExponentialHistogramCode int8 = 5
)

// UnivariateMetricDT is the Arrow Data Type describing a univariate metric.
var (
	UnivariateMetricDT = arrow.SparseUnionOf([]arrow.Field{
		{Name: constants.GAUGE_METRICS, Type: UnivariateGaugeDT},
		{Name: constants.SUM_METRICS, Type: UnivariateSumDT},
		{Name: constants.SUMMARY_METRICS, Type: UnivariateSummaryDT},
		// {Name: constants.HISTOGRAM_METRICS, Type: UnivariateHistogramDT},
		// {Name: constants.EXPONENTIAL_HISTOGRAM_METRICS, Type: UnivariateExponentialHistogramDT},
	},
		[]arrow.UnionTypeCode{
			GaugeCode,
			SumCode,
			SummaryCode,
		},
	)
)

// UnivariateMetricBuilder is a builder for univariate metrics.
type UnivariateMetricBuilder struct {
	released bool

	builder *array.SparseUnionBuilder

	gb  *UnivariateGaugeBuilder   // univariate gauge builder
	sb  *UnivariateSumBuilder     // univariate sum builder
	syb *UnivariateSummaryBuilder // univariate summary builder
}

// NewUnivariateMetricBuilder creates a new UnivariateMetricBuilder with a given memory allocator.
func NewUnivariateMetricBuilder(pool memory.Allocator) *UnivariateMetricBuilder {
	return UnivariateMetricBuilderFrom(array.NewSparseUnionBuilder(pool, UnivariateMetricDT))
}

// UnivariateMetricBuilderFrom creates a new UnivariateMetricBuilder from an existing StructBuilder.
func UnivariateMetricBuilderFrom(umb *array.SparseUnionBuilder) *UnivariateMetricBuilder {
	return &UnivariateMetricBuilder{
		released: false,
		builder:  umb,

		gb:  UnivariateGaugeBuilderFrom(umb.Child(0).(*array.StructBuilder)),
		sb:  UnivariateSumBuilderFrom(umb.Child(1).(*array.StructBuilder)),
		syb: UnivariateSummaryBuilderFrom(umb.Child(2).(*array.StructBuilder)),
	}
}

// Build builds the underlying array.
//
// Once the array is no longer needed, Release() should be called to free the memory.
func (b *UnivariateMetricBuilder) Build() (*array.SparseUnion, error) {
	if b.released {
		return nil, fmt.Errorf("UnivariateMetricBuilder: Build() called after Release()")
	}

	defer b.Release()
	return b.builder.NewSparseUnionArray(), nil
}

// Release releases the underlying memory.
func (b *UnivariateMetricBuilder) Release() {
	if b.released {
		return
	}

	b.released = true
	b.builder.Release()
}

// Append appends a new univariate gauge to the builder.
func (b *UnivariateMetricBuilder) Append(gauge pmetric.Metric) error {
	if b.released {
		return fmt.Errorf("UnivariateMetricBuilder: Append() called after Release()")
	}

	// TODO

	return nil
}
