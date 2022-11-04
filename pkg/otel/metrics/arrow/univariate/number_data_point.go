package univariate

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"

	common_arrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	metric_arrow "github.com/f5/otel-arrow-adapter/pkg/otel/metrics/arrow"
)

// NumberDataPointDT is the Arrow Data Type describing a univariate number data point.
var (
	NumberDataPointDT = arrow.StructOf([]arrow.Field{
		{Name: constants.ATTRIBUTES, Type: common_arrow.AttributesDT},
		{Name: constants.START_TIME_UNIX_NANO, Type: arrow.PrimitiveTypes.Uint64},
		{Name: constants.TIME_UNIX_NANO, Type: arrow.PrimitiveTypes.Uint64},
		{Name: constants.METRIC_VALUE, Type: metric_arrow.MetricValueDT},
		// Exemplars
		{Name: constants.FLAGS, Type: arrow.PrimitiveTypes.Uint32},
	}...)
)

// GaugeBuilder is a builder for gauge metrics.
type GaugeBuilder struct {
	released bool

	builder *array.StructBuilder

	vb *array.Float64Builder
	fb *array.Uint32Builder
}
