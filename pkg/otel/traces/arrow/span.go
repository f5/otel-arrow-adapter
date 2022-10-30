package arrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// SpanDT is the Arrow Data Type describing a span.
var (
	SpanDT = arrow.StructOf([]arrow.Field{
		{Name: constants.START_TIME_UNIX_NANO, Type: arrow.PrimitiveTypes.Uint64},
		{Name: constants.END_TIME_UNIX_NANO, Type: arrow.PrimitiveTypes.Uint64},
		{Name: constants.TRACE_ID, Type: arrow.BinaryTypes.Binary},
		{Name: constants.SPAN_ID, Type: arrow.BinaryTypes.Binary},
		{Name: constants.TRACE_STATE, Type: arrow.BinaryTypes.String},
		{Name: constants.PARENT_SPAN_ID, Type: arrow.BinaryTypes.Binary},
		{Name: constants.NAME, Type: arrow.BinaryTypes.String},
		{Name: constants.KIND, Type: arrow.PrimitiveTypes.Int32},
		{Name: constants.ATTRIBUTES, Type: acommon.AttributesDT},
		{Name: constants.DROPPED_ATTRIBUTES_COUNT, Type: arrow.PrimitiveTypes.Uint32},
		{Name: constants.SPAN_EVENTS, Type: arrow.ListOf(EventDT)},
		{Name: constants.DROPPED_EVENTS_COUNT, Type: arrow.PrimitiveTypes.Uint32},
		{Name: constants.SPAN_LINKS, Type: arrow.ListOf(LinkDT)},
		{Name: constants.DROPPED_LINKS_COUNT, Type: arrow.PrimitiveTypes.Uint32},
		{Name: constants.STATUS, Type: StatusDT},
	}...)
)

// ResourceSpansBuilder is a helper to build a span.
type SpanBuilder struct {
	released bool

	builder *array.StructBuilder

	stunb *array.Uint64Builder
	etunb *array.Uint64Builder
	tib   *array.BinaryBuilder
	sib   *array.BinaryBuilder
	tsb   *array.StringBuilder
	psib  *array.BinaryBuilder
	nb    *array.StringBuilder
	kb    *array.Int32Builder
	ab    *acommon.AttributesBuilder
	dacb  *array.Uint32Builder
	sesb  *array.ListBuilder
	seb   *EventBuilder
	decb  *array.Uint32Builder
	slsb  *array.ListBuilder
	slb   *LinkBuilder
	dlcb  *array.Uint32Builder
	sb    *StatusBuilder
}

// NewScopeSpansBuilder creates a new ResourceSpansBuilder with a given allocator.
//
// Once the builder is no longer needed, Release() must be called to free the
// memory allocated by the builder.
func NewSpanBuilder(pool *memory.GoAllocator) *SpanBuilder {
	sb := array.NewStructBuilder(pool, SpanDT)
	return SpanBuilderFrom(sb)
}

func SpanBuilderFrom(sb *array.StructBuilder) *SpanBuilder {
	return &SpanBuilder{
		released: false,
		builder:  sb,
		stunb:    sb.FieldBuilder(0).(*array.Uint64Builder),
		etunb:    sb.FieldBuilder(1).(*array.Uint64Builder),
		tib:      sb.FieldBuilder(2).(*array.BinaryBuilder),
		sib:      sb.FieldBuilder(3).(*array.BinaryBuilder),
		tsb:      sb.FieldBuilder(4).(*array.StringBuilder),
		psib:     sb.FieldBuilder(5).(*array.BinaryBuilder),
		nb:       sb.FieldBuilder(6).(*array.StringBuilder),
		kb:       sb.FieldBuilder(7).(*array.Int32Builder),
		ab:       acommon.AttributesBuilderFrom(sb.FieldBuilder(8).(*array.MapBuilder)),
		dacb:     sb.FieldBuilder(9).(*array.Uint32Builder),
		sesb:     sb.FieldBuilder(10).(*array.ListBuilder),
		seb:      EventBuilderFrom(sb.FieldBuilder(10).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
		decb:     sb.FieldBuilder(11).(*array.Uint32Builder),
		slsb:     sb.FieldBuilder(12).(*array.ListBuilder),
		slb:      LinkBuilderFrom(sb.FieldBuilder(12).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
		dlcb:     sb.FieldBuilder(13).(*array.Uint32Builder),
		sb:       StatusBuilderFrom(sb.FieldBuilder(14).(*array.StructBuilder)),
	}
}

// Build builds the span array map.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *SpanBuilder) Build() *array.Struct {
	if b.released {
		panic("span builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray()
}

// Append appends a new span to the builder.
//
// This method panics if the builder has already been released.
func (b *SpanBuilder) Append(span ptrace.Span) {
	if b.released {
		panic("attribute builder already released")
	}

	b.builder.Append(true)
	b.stunb.Append(uint64(span.StartTimestamp()))
	b.etunb.Append(uint64(span.EndTimestamp()))
	tib := span.TraceID()
	b.tib.Append(tib[:])
	sib := span.SpanID()
	b.sib.Append(sib[:])
	b.tsb.Append(span.TraceState().AsRaw())
	psib := span.ParentSpanID()
	b.psib.Append(psib[:])
	b.nb.Append(span.Name())
	b.kb.Append(int32(span.Kind()))
	b.ab.Append(span.Attributes())
	b.dacb.Append(uint32(span.DroppedAttributesCount()))
	evts := span.Events()
	sc := evts.Len()
	if sc > 0 {
		b.sesb.Append(true)
		b.sesb.Reserve(sc)
		for i := 0; i < sc; i++ {
			b.seb.Append(evts.At(i))
		}
	} else {
		b.sesb.Append(false)
	}
	b.decb.Append(span.DroppedEventsCount())
	links := span.Links()
	lc := links.Len()
	if lc > 0 {
		b.slsb.Append(true)
		b.slsb.Reserve(lc)
		for i := 0; i < lc; i++ {
			b.slb.Append(links.At(i))
		}
	} else {
		b.slsb.Append(false)
	}
	b.dlcb.Append(span.DroppedLinksCount())
	b.sb.Append(span.Status())
}

// Release releases the memory allocated by the builder.
func (b *SpanBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.stunb.Release()
		b.etunb.Release()
		b.tib.Release()
		b.sib.Release()
		b.tsb.Release()
		b.psib.Release()
		b.nb.Release()
		b.kb.Release()
		b.ab.Release()
		b.dacb.Release()
		b.sesb.Release()
		b.seb.Release()
		b.decb.Release()
		b.slsb.Release()
		b.slb.Release()
		b.dlcb.Release()
		b.sb.Release()

		b.released = true
	}
}
