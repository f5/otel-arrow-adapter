package arrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// ResourceDT is the Arrow Data Type describing a resource.
var (
	ResourceDT = arrow.StructOf([]arrow.Field{
		{Name: constants.ATTRIBUTES, Type: AttributesDT},
		{Name: constants.DROPPED_ATTRIBUTES_COUNT, Type: arrow.PrimitiveTypes.Uint32},
	}...)
)

type ResourceBuilder struct {
	released bool
	builder  *array.StructBuilder
	ab       *AttributesBuilder
	dacb     *array.Uint32Builder
}

func NewResourceBuilder(pool *memory.GoAllocator) *ResourceBuilder {
	return ResourceBuilderFrom(array.NewStructBuilder(pool, ResourceDT))
}

func ResourceBuilderFrom(rb *array.StructBuilder) *ResourceBuilder {
	return &ResourceBuilder{
		released: false,
		builder:  rb,
		ab:       AttributesBuilderFrom(rb.FieldBuilder(0).(*array.MapBuilder)),
		dacb:     rb.FieldBuilder(1).(*array.Uint32Builder),
	}
}

// Append appends a new resource to the builder.
//
// This method panics if the builder has already been released.
func (b *ResourceBuilder) Append(resource pcommon.Resource) {
	if b.released {
		panic("resource builder already released")
	}

	b.builder.Append(true)
	b.ab.Append(resource.Attributes())
	b.dacb.Append(resource.DroppedAttributesCount())
}

// Build builds the resource array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ResourceBuilder) Build() *array.Struct {
	defer b.Release()
	return b.builder.NewStructArray()
}

// Release releases the memory allocated by the builder.
func (b *ResourceBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.ab.Release()
		b.dacb.Release()

		b.released = true
	}
}
