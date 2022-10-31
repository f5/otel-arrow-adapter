package arrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// LinkDT is the Arrow Data Type describing a link event.
var (
	LinkDT = arrow.StructOf([]arrow.Field{
		{Name: constants.TRACE_ID, Type: arrow.BinaryTypes.Binary},
		{Name: constants.SPAN_ID, Type: arrow.BinaryTypes.Binary},
		{Name: constants.TRACE_STATE, Type: acommon.Dict16String},
		{Name: constants.ATTRIBUTES, Type: acommon.AttributesDT},
		{Name: constants.DROPPED_ATTRIBUTES_COUNT, Type: arrow.PrimitiveTypes.Uint32},
	}...)
)

type LinkBuilder struct {
	released bool
	builder  *array.StructBuilder
	tib      *array.BinaryBuilder
	sib      *array.BinaryBuilder
	tsb      *array.BinaryDictionaryBuilder
	ab       *acommon.AttributesBuilder
	dacb     *array.Uint32Builder
}

func NewLinkBuilder(pool *memory.GoAllocator) *LinkBuilder {
	return LinkBuilderFrom(array.NewStructBuilder(pool, LinkDT))
}

func LinkBuilderFrom(lb *array.StructBuilder) *LinkBuilder {
	return &LinkBuilder{
		released: false,
		builder:  lb,
		tib:      lb.FieldBuilder(0).(*array.BinaryBuilder),
		sib:      lb.FieldBuilder(1).(*array.BinaryBuilder),
		tsb:      lb.FieldBuilder(2).(*array.BinaryDictionaryBuilder),
		ab:       acommon.AttributesBuilderFrom(lb.FieldBuilder(3).(*array.MapBuilder)),
		dacb:     lb.FieldBuilder(4).(*array.Uint32Builder),
	}
}

// Append appends a new link to the builder.
//
// This method panics if the builder has already been released.
func (b *LinkBuilder) Append(link ptrace.SpanLink) {
	if b.released {
		panic("link builder already released")
	}

	b.builder.Append(true)
	tid := link.TraceID()
	b.tib.Append(tid[:])
	sid := link.SpanID()
	b.sib.Append(sid[:])
	b.tsb.AppendString(link.TraceState().AsRaw())
	b.ab.Append(link.Attributes())
	b.dacb.Append(link.DroppedAttributesCount())
}

// Build builds the link array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *LinkBuilder) Build() *array.Struct {
	if b.released {
		panic("link builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray()
}

// Release releases the memory allocated by the builder.
func (b *LinkBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.tib.Release()
		b.sib.Release()
		b.tsb.Release()
		b.ab.Release()
		b.dacb.Release()

		b.released = true
	}
}
