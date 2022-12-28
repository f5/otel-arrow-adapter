// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

var (
	ResourceSpansDT = arrow.StructOf([]arrow.Field{
		{Name: constants.Resource, Type: acommon.ResourceDT},
		{Name: constants.SchemaUrl, Type: acommon.DefaultDictString},
		{Name: constants.ScopeSpans, Type: arrow.ListOf(ScopeSpansDT)},
	}...)
)

// ResourceSpansBuilder is a helper to build resource spans.
type ResourceSpansBuilder struct {
	released bool

	builder *array.StructBuilder // builder for the resource spans struct

	rb   *acommon.ResourceBuilder           // resource builder
	schb *acommon.AdaptiveDictionaryBuilder // schema url builder
	spsb *array.ListBuilder                 // scope span list builder
	spb  *ScopeSpansBuilder                 // scope span builder
}

// NewResourceSpansBuilder creates a new ResourceSpansBuilder with a given allocator.
//
// Once the builder is no longer needed, Build() or Release() must be called to free the
// memory allocated by the builder.
func NewResourceSpansBuilder(pool memory.Allocator) *ResourceSpansBuilder {
	builder := array.NewStructBuilder(pool, ResourceSpansDT)
	return ResourceSpansBuilderFrom(builder)
}

// ResourceSpansBuilderFrom creates a new ResourceSpansBuilder from an existing builder.
func ResourceSpansBuilderFrom(builder *array.StructBuilder) *ResourceSpansBuilder {
	return &ResourceSpansBuilder{
		released: false,
		builder:  builder,
		rb:       acommon.ResourceBuilderFrom(builder.FieldBuilder(0).(*array.StructBuilder)),
		schb:     acommon.AdaptiveDictionaryBuilderFrom(builder.FieldBuilder(1)),
		spsb:     builder.FieldBuilder(2).(*array.ListBuilder),
		spb:      ScopeSpansBuilderFrom(builder.FieldBuilder(2).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
	}
}

// Build builds the resource spans array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ResourceSpansBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("resource spans builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Append appends a new resource spans to the builder.
func (b *ResourceSpansBuilder) Append(ss ptrace.ResourceSpans) error {
	if b.released {
		return fmt.Errorf("resource spans builder already released")
	}

	b.builder.Append(true)
	if err := b.rb.Append(ss.Resource()); err != nil {
		return err
	}
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
			if err := b.spb.Append(sspans.At(i)); err != nil {
				return err
			}
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

		b.released = true
	}
}
