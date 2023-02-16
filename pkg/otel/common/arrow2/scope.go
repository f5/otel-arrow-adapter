/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package arrow2

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// ScopeDT is the Arrow Data Type describing a scope.
var (
	ScopeDT = arrow.StructOf([]arrow.Field{
		{Name: constants.Name, Type: arrow.BinaryTypes.String, Metadata: acommon.Metadata(acommon.Optional, acommon.Dictionary)},
		{Name: constants.Version, Type: arrow.BinaryTypes.String, Metadata: acommon.Metadata(acommon.Optional, acommon.Dictionary)},
		{Name: constants.Attributes, Type: AttributesDT, Metadata: acommon.Metadata(acommon.Optional)},
		{Name: constants.DroppedAttributesCount, Type: arrow.PrimitiveTypes.Uint32, Metadata: acommon.Metadata(acommon.Optional)},
	}...)
)

type ScopeBuilder struct {
	released bool
	builder  *builder.StructBuilder
	nb       *builder.StringBuilder // Name builder
	vb       *builder.StringBuilder // Version builder
	ab       *AttributesBuilder     // Attributes builder
	dacb     *builder.Uint32Builder // Dropped attributes count builder
}

// NewScopeBuilder creates a new instrumentation scope array builder with a given allocator.
func NewScopeBuilder(builder *builder.StructBuilder) *ScopeBuilder {
	return ScopeBuilderFrom(builder)
}

// ScopeBuilderFrom creates a new instrumentation scope array builder from an existing struct builder.
func ScopeBuilderFrom(sb *builder.StructBuilder) *ScopeBuilder {
	return &ScopeBuilder{
		released: false,
		builder:  sb,
		nb:       sb.StringBuilder(constants.Name),
		vb:       sb.StringBuilder(constants.Version),
		ab:       AttributesBuilderFrom(sb.MapBuilder(constants.Attributes)),
		dacb:     sb.Uint32Builder(constants.DroppedAttributesCount),
	}
}

// Append appends a new instrumentation scope to the builder.
func (b *ScopeBuilder) Append(scope pcommon.InstrumentationScope) error {
	if b.released {
		return fmt.Errorf("scope builder already released")
	}

	return b.builder.Append(scope, func() error {
		b.nb.Append(scope.Name())
		b.vb.Append(scope.Version())
		if err := b.ab.Append(scope.Attributes()); err != nil {
			return err
		}
		b.dacb.AppendNonZero(scope.DroppedAttributesCount())
		return nil
	})
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
