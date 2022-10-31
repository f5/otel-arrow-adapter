package arrow

import (
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TracesBuilder is a helper to build a list of resource spans.
type TracesBuilder struct {
	released bool

	builder *array.ListBuilder
	rsp     *ResourceSpansBuilder
}

// NewTracesBuilder creates a new TracesBuilder with a given allocator.
func NewTracesBuilder(pool *memory.GoAllocator) *TracesBuilder {
	rsb := array.NewListBuilder(pool, ResourceSpansDT)
	return &TracesBuilder{
		released: false,
		builder:  rsb,
		rsp:      ResourceSpansBuilderFrom(rsb.ValueBuilder().(*array.StructBuilder)),
	}
}

// Build builds the resource spans list.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *TracesBuilder) Build() *array.List {
	defer b.Release()
	return b.builder.NewListArray()
}

// Append appends a new set of resource spans to the builder.
//
// This method panics if the builder has already been released.
func (b *TracesBuilder) Append(traces ptrace.Traces) {
	if b.released {
		panic("traces builder already released")
	}

	rs := traces.ResourceSpans()
	rc := rs.Len()
	if rc > 0 {
		b.builder.Append(true)
		b.builder.Reserve(rc)
		for i := 0; i < rc; i++ {
			b.rsp.Append(rs.At(i))
		}
	} else {
		b.builder.AppendNull()
	}
}

// Release releases the memory allocated by the builder.
func (b *TracesBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.rsp.Release()
		b.released = true
	}
}
