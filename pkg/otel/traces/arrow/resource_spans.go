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
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

var (
	// ResourceSpansDT is the data type for resource spans.
	ResourceSpansDT = arrow.StructOf([]arrow.Field{
		{Name: constants.Resource, Type: acommon.ResourceDT, Metadata: schema.OptionalField},
		{Name: constants.SchemaUrl, Type: acommon.DefaultDictString, Metadata: schema.OptionalField},
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
	resourceBuilder := schema.StructFieldBuilder(ResourceSpansDT, constants.Resource, builder)
	schemaUrlBuilder := schema.StructFieldBuilder(ResourceSpansDT, constants.SchemaUrl, builder)
	scopeSpansBuilder := schema.StructFieldBuilder(ResourceSpansDT, constants.ScopeSpans, builder)

	return &ResourceSpansBuilder{
		released: false,
		builder:  builder,
		rb:       acommon.ResourceBuilderFrom(resourceBuilder.(*array.StructBuilder)),
		schb:     acommon.AdaptiveDictionaryBuilderFrom(schemaUrlBuilder),
		spsb:     scopeSpansBuilder.(*array.ListBuilder),
		spb:      ScopeSpansBuilderFrom(scopeSpansBuilder.(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
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

	// resource (optional)
	resource := ss.Resource()
	if resource.DroppedAttributesCount() > 0 && b.rb == nil {

	}
	if b.rb != nil {
		if err := b.rb.Append(resource); err != nil {
			return err
		}
	}

	// schema url (optional)
	if b.schb != nil {
		schemaUrl := ss.SchemaUrl()
		if schemaUrl == "" {
			b.schb.AppendNull()
		} else {
			if err := b.schb.AppendString(schemaUrl); err != nil {
				return err
			}
		}
	}

	// scope spans (mandatory)
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
