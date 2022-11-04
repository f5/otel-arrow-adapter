package arrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
)

// GaugeDT is the Arrow Data Type describing a gauge.
var (
	GaugeDT = arrow.StructOf([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "flags", Type: arrow.PrimitiveTypes.Uint32},
	}...)
)

// GaugeBuilder is a builder for gauge metrics.
type GaugeBuilder struct {
	released bool

	builder *array.StructBuilder

	vb *array.Float64Builder
	fb *array.Uint32Builder
}
