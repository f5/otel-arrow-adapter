package arrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// ScopeSpansDT is the Arrow Data Type describing a scope span.
var (
	ScopeSpansDT = arrow.StructOf([]arrow.Field{
		{Name: constants.SCOPE, Type: acommon.ScopeDT},
		{Name: constants.SCHEMA_URL, Type: arrow.BinaryTypes.String},
		{Name: constants.SPANS, Type: arrow.ListOf(SpanDT)},
	}...)
)

// ResourceSpansBuilder is a helper to build a scope spans.
type ScopeSpansBuilder struct {
	released bool

	builder *array.StructBuilder

	scb  *acommon.ScopeBuilder
	schb *array.StringBuilder
	ssb  *array.ListBuilder
	sb   *SpanBuilder
}

// NewScopeSpansBuilder creates a new ResourceSpansBuilder with a given allocator.
//
// Once the builder is no longer needed, Release() must be called to free the
// memory allocated by the builder.
func NewScopeSpansBuilder(pool *memory.GoAllocator) *ScopeSpansBuilder {
	builder := array.NewStructBuilder(pool, ScopeSpansDT)
	return ScopeSpansBuilderFrom(builder)
}

func ScopeSpansBuilderFrom(builder *array.StructBuilder) *ScopeSpansBuilder {
	return &ScopeSpansBuilder{
		released: false,
		builder:  builder,
		scb:      acommon.ScopeBuilderFrom(builder.FieldBuilder(0).(*array.StructBuilder)),
		schb:     builder.FieldBuilder(1).(*array.StringBuilder),
		ssb:      builder.FieldBuilder(2).(*array.ListBuilder),
		sb:       SpanBuilderFrom(builder.FieldBuilder(2).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
	}
}

// Build builds the scope spans array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ScopeSpansBuilder) Build() *array.Struct {
	if b.released {
		panic("scope spans builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray()
}

// Append appends a new scope spans to the builder.
//
// This method panics if the builder has already been released.
func (b *ScopeSpansBuilder) Append(ss ptrace.ScopeSpans) {
	if b.released {
		panic("scope spans builder already released")
	}

	b.builder.Append(true)
	b.scb.Append(ss.Scope())
	b.schb.Append(ss.SchemaUrl())
	spans := ss.Spans()
	sc := spans.Len()
	if sc > 0 {
		b.ssb.Append(true)
		b.ssb.Reserve(sc)
		for i := 0; i < sc; i++ {
			b.sb.Append(spans.At(i))
		}
	} else {
		b.ssb.Append(false)
	}
}

// Release releases the memory allocated by the builder.
func (b *ScopeSpansBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.scb.Release()
		b.schb.Release()
		b.ssb.Release()
		b.sb.Release()

		b.released = true
	}
}
