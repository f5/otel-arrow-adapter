package arrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// ScopeDT is the Arrow Data Type describing a scope.
var (
	ScopeDT = arrow.StructOf([]arrow.Field{
		{Name: constants.NAME, Type: Dict16String},
		// TODO: should be a dictionary
		{Name: constants.VERSION, Type: arrow.BinaryTypes.String},
		{Name: constants.ATTRIBUTES, Type: AttributesDT},
		{Name: constants.DROPPED_ATTRIBUTES_COUNT, Type: arrow.PrimitiveTypes.Uint32},
	}...)
)

type ScopeBuilder struct {
	released bool
	builder  *array.StructBuilder
	nb       *array.BinaryDictionaryBuilder
	vb       *array.StringBuilder
	ab       *AttributesBuilder
	dacb     *array.Uint32Builder
}

func NewScopeBuilder(pool *memory.GoAllocator) *ScopeBuilder {
	return ScopeBuilderFrom(array.NewStructBuilder(pool, ScopeDT))
}

func ScopeBuilderFrom(sb *array.StructBuilder) *ScopeBuilder {
	return &ScopeBuilder{
		released: false,
		builder:  sb,
		nb:       sb.FieldBuilder(0).(*array.BinaryDictionaryBuilder),
		vb:       sb.FieldBuilder(1).(*array.StringBuilder),
		ab:       AttributesBuilderFrom(sb.FieldBuilder(2).(*array.MapBuilder)),
		dacb:     sb.FieldBuilder(3).(*array.Uint32Builder),
	}
}

// Append appends a new instrumentation scope to the builder.
//
// This method panics if the builder has already been released.
func (b *ScopeBuilder) Append(resource pcommon.InstrumentationScope) {
	if b.released {
		panic("scope builder already released")
	}

	b.builder.Append(true)
	b.nb.AppendString(resource.Name())
	b.vb.Append(resource.Version())
	b.ab.Append(resource.Attributes())
	b.dacb.Append(resource.DroppedAttributesCount())
}

// Build builds the instrumentation scope array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ScopeBuilder) Build() *array.Struct {
	if b.released {
		panic("scope builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray()
}

// Release releases the memory allocated by the builder.
func (b *ScopeBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.nb.Release()
		b.vb.Release()
		b.ab.Release()
		b.dacb.Release()

		b.released = true
	}
}
