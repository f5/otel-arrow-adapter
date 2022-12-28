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
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// ScopeDT is the Arrow Data Type describing a scope.
var (
	ScopeDT = arrow.StructOf([]arrow.Field{
		{Name: constants.NAME, Type: DefaultDictString},
		{Name: constants.VERSION, Type: DefaultDictString},
		{Name: constants.ATTRIBUTES, Type: AttributesDT},
		{Name: constants.DROPPED_ATTRIBUTES_COUNT, Type: arrow.PrimitiveTypes.Uint32},
	}...)
)

type ScopeBuilder struct {
	released bool
	builder  *array.StructBuilder
	nb       *AdaptiveDictionaryBuilder // Name builder
	vb       *AdaptiveDictionaryBuilder // Version builder
	ab       *AttributesBuilder         // Attributes builder
	dacb     *array.Uint32Builder       // Dropped attributes count builder
}

// NewScopeBuilder creates a new instrumentation scope array builder with a given allocator.
func NewScopeBuilder(pool memory.Allocator) *ScopeBuilder {
	return ScopeBuilderFrom(array.NewStructBuilder(pool, ScopeDT))
}

// ScopeBuilderFrom creates a new instrumentation scope array builder from an existing struct builder.
func ScopeBuilderFrom(sb *array.StructBuilder) *ScopeBuilder {
	return &ScopeBuilder{
		released: false,
		builder:  sb,
		nb:       AdaptiveDictionaryBuilderFrom(sb.FieldBuilder(0)),
		vb:       AdaptiveDictionaryBuilderFrom(sb.FieldBuilder(1)),
		ab:       AttributesBuilderFrom(sb.FieldBuilder(2).(*array.MapBuilder)),
		dacb:     sb.FieldBuilder(3).(*array.Uint32Builder),
	}
}

// Append appends a new instrumentation scope to the builder.
func (b *ScopeBuilder) Append(scope pcommon.InstrumentationScope) error {
	if b.released {
		return fmt.Errorf("scope builder already released")
	}

	b.builder.Append(true)
	name := scope.Name()
	if name == "" {
		b.nb.AppendNull()
	} else {
		if err := b.nb.AppendString(name); err != nil {
			return err
		}
	}
	version := scope.Version()
	if version == "" {
		b.vb.AppendNull()
	} else {
		if err := b.vb.AppendString(version); err != nil {
			return err
		}
	}
	if err := b.ab.Append(scope.Attributes()); err != nil {
		return err
	}
	if scope.DroppedAttributesCount() > 0 {
		b.dacb.Append(scope.DroppedAttributesCount())
	} else {
		b.dacb.AppendNull()
	}
	return nil
}

// Build builds the instrumentation scope array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ScopeBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("scope builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the memory allocated by the builder.
func (b *ScopeBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
