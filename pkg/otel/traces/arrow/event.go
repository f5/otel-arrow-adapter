package arrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// EventDT is the Arrow Data Type describing a span event.
var (
	EventDT = arrow.StructOf([]arrow.Field{
		{Name: constants.TIME_UNIX_NANO, Type: arrow.PrimitiveTypes.Uint64},
		{Name: constants.NAME, Type: arrow.BinaryTypes.String},
		{Name: constants.ATTRIBUTES, Type: acommon.AttributesDT},
		{Name: constants.DROPPED_ATTRIBUTES_COUNT, Type: arrow.PrimitiveTypes.Uint32},
	}...)
)

type EventBuilder struct {
	released bool
	builder  *array.StructBuilder
	tunb     *array.Uint64Builder
	nb       *array.StringBuilder
	ab       *acommon.AttributesBuilder
	dacb     *array.Uint32Builder
}

func NewEventBuilder(pool *memory.GoAllocator) *EventBuilder {
	return EventBuilderFrom(array.NewStructBuilder(pool, EventDT))
}

func EventBuilderFrom(eb *array.StructBuilder) *EventBuilder {
	return &EventBuilder{
		released: false,
		builder:  eb,
		tunb:     eb.FieldBuilder(0).(*array.Uint64Builder),
		nb:       eb.FieldBuilder(1).(*array.StringBuilder),
		ab:       acommon.AttributesBuilderFrom(eb.FieldBuilder(2).(*array.MapBuilder)),
		dacb:     eb.FieldBuilder(3).(*array.Uint32Builder),
	}
}

// Append appends a new event to the builder.
//
// This method panics if the builder has already been released.
func (b *EventBuilder) Append(event ptrace.SpanEvent) {
	if b.released {
		panic("event builder already released")
	}

	b.builder.Append(true)
	b.tunb.Append(uint64(event.Timestamp()))
	b.nb.Append(event.Name())
	b.ab.Append(event.Attributes())
	b.dacb.Append(event.DroppedAttributesCount())
}

// Build builds the event array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *EventBuilder) Build() *array.Struct {
	if b.released {
		panic("event builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray()
}

// Release releases the memory allocated by the builder.
func (b *EventBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.tunb.Release()
		b.nb.Release()
		b.ab.Release()
		b.dacb.Release()

		b.released = true
	}
}
