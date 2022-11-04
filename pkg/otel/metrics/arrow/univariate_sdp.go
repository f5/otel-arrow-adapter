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

// QuantileValueDT is the Arrow Data Type describing a univariate number data point.
var (
	UnivariateSummaryDataPointDT = arrow.StructOf(
		arrow.Field{Name: constants.ATTRIBUTES, Type: acommon.AttributesDT},
		arrow.Field{Name: constants.START_TIME_UNIX_NANO, Type: arrow.PrimitiveTypes.Uint64},
		arrow.Field{Name: constants.TIME_UNIX_NANO, Type: arrow.PrimitiveTypes.Uint64},
		arrow.Field{Name: constants.SUMMARY_COUNT, Type: arrow.PrimitiveTypes.Uint64},
		arrow.Field{Name: constants.SUMMARY_SUM, Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: constants.SUMMARY_QUANTILE_VALUES, Type: arrow.ListOf(QuantileValueDT)},
		arrow.Field{Name: constants.FLAGS, Type: arrow.PrimitiveTypes.Uint32},
	)
)

// QuantileValueBuilder is a builder for a summary data point.
type SummaryDataPointBuilder struct {
	released bool

	builder *array.StructBuilder

	ab    *acommon.AttributesBuilder // attributes builder
	stunb *array.Uint64Builder       // start_time_unix_nano builder
	tunb  *array.Uint64Builder       // time_unix_nano builder
	mvb   *MetricValueBuilder        // metric_value builder
	elb   *array.ListBuilder         // exemplars builder
	eb    *ExemplarBuilder           // exemplar builder
	fb    *array.Uint32Builder       // flags builder
}

// NewQuantileValueBuilder creates a new QuantileValueBuilder with a given memory allocator.
func NewSummaryDataPointBuilder(pool memory.Allocator) *SummaryDataPointBuilder {
	return SummaryDataPointBuilderFrom(array.NewStructBuilder(pool, UnivariateSummaryDataPointDT))
}

// QuantileValueBuilderFrom creates a new QuantileValueBuilder from an existing StructBuilder.
func SummaryDataPointBuilderFrom(ndpb *array.StructBuilder) *SummaryDataPointBuilder {
	return &SummaryDataPointBuilder{
		released: false,
		builder:  ndpb,

		ab:    acommon.AttributesBuilderFrom(ndpb.FieldBuilder(0).(*array.MapBuilder)),
		stunb: ndpb.FieldBuilder(1).(*array.Uint64Builder),
		tunb:  ndpb.FieldBuilder(2).(*array.Uint64Builder),
		mvb:   MetricValueBuilderFrom(ndpb.FieldBuilder(3).(*array.DenseUnionBuilder)),
		elb:   ndpb.FieldBuilder(4).(*array.ListBuilder),
		eb:    ExemplarBuilderFrom(ndpb.FieldBuilder(4).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
		fb:    ndpb.FieldBuilder(5).(*array.Uint32Builder),
	}
}

// Build builds the underlying array.
//
// Once the array is no longer needed, Release() should be called to free the memory.
func (b *SummaryDataPointBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("QuantileValueBuilder: Build() called after Release()")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the underlying memory.
func (b *SummaryDataPointBuilder) Release() {
	if b.released {
		return
	}

	b.released = true
	b.builder.Release()
}

// Append appends a new summary data point to the builder.
func (b *SummaryDataPointBuilder) Append(sdp pmetric.SummaryDataPoint) error {
	if b.released {
		return fmt.Errorf("QuantileValueBuilder: Append() called after Release()")
	}

	b.builder.Append(true)
	if err := b.ab.Append(sdp.Attributes()); err != nil {
		return err
	}
	b.stunb.Append(uint64(sdp.StartTimestamp()))
	b.tunb.Append(uint64(sdp.Timestamp()))
	//if err := b.mvb.AppendNumberDataPointValue(sdp); err != nil {
	//	return err
	//}
	b.fb.Append(uint32(sdp.Flags()))
	return nil
}
