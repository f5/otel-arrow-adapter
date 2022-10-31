package arrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

var (
	ResourceSpansDT = arrow.StructOf([]arrow.Field{
		{Name: constants.RESOURCE, Type: acommon.ResourceDT},
		{Name: constants.SCHEMA_URL, Type: acommon.Dict16String},
		{Name: constants.SCOPE_SPANS, Type: arrow.ListOf(ScopeSpansDT)},
	}...)
)

// ResourceSpansBuilder is a helper to build a resource spans.
type ResourceSpansBuilder struct {
	released bool

	builder *array.StructBuilder

	rb   *acommon.ResourceBuilder       // resource builder
	schb *array.BinaryDictionaryBuilder // schema url builder
	spsb *array.ListBuilder             // scope span list builder
	spb  *ScopeSpansBuilder             // scope span builder
}

// NewResourceSpansBuilder creates a new ResourceSpansBuilder with a given allocator.
//
// Once the builder is no longer needed, Release() must be called to free the
// memory allocated by the builder.
func NewResourceSpansBuilder(pool *memory.GoAllocator) *ResourceSpansBuilder {
	builder := array.NewStructBuilder(pool, ResourceSpansDT)
	return ResourceSpansBuilderFrom(builder)
}

func ResourceSpansBuilderFrom(builder *array.StructBuilder) *ResourceSpansBuilder {
	return &ResourceSpansBuilder{
		released: false,
		builder:  builder,
		rb:       acommon.ResourceBuilderFrom(builder.FieldBuilder(0).(*array.StructBuilder)),
		schb:     builder.FieldBuilder(1).(*array.BinaryDictionaryBuilder),
		spsb:     builder.FieldBuilder(2).(*array.ListBuilder),
		spb:      ScopeSpansBuilderFrom(builder.FieldBuilder(2).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
	}
}

// Build builds the resource spans array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ResourceSpansBuilder) Build() *array.Struct {
	if b.released {
		panic("resource spans builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray()
}

// Append appends a new resource spans to the builder.
//
// This method panics if the builder has already been released.
func (b *ResourceSpansBuilder) Append(ss ptrace.ResourceSpans) error {
	if b.released {
		panic("resource spans builder already released")
	}

	b.builder.Append(true)
	b.rb.Append(ss.Resource())
	schemaUrl := ss.SchemaUrl()
	if schemaUrl == "" {
		b.schb.AppendNull()
	} else {
		if err := b.schb.AppendString(schemaUrl); err != nil {
			return err
		}
	}
	sspans := ss.ScopeSpans()
	sc := sspans.Len()
	if sc > 0 {
		b.spsb.Append(true)
		b.spsb.Reserve(sc)
		for i := 0; i < sc; i++ {
			b.spb.Append(sspans.At(i))
		}
	} else {
		b.spsb.Append(false)
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *ResourceSpansBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.rb.Release()
		b.schb.Release()
		b.spsb.Release()
		b.spb.Release()

		b.released = true
	}
}
