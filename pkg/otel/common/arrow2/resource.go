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
	"github.com/apache/arrow/go/v11/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	cfg "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/config"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// ResourceDT is the Arrow Data Type describing a resource.
var (
	ResourceDT = arrow.StructOf([]arrow.Field{
		{
			Name:     constants.Attributes,
			Type:     AttributesDT,
			Metadata: acommon.Metadata(acommon.Optional),
		},
		{
			Name:     constants.DroppedAttributesCount,
			Type:     arrow.PrimitiveTypes.Uint32,
			Metadata: acommon.Metadata(acommon.Optional),
		},
	}...)
)

// ResourceBuilder is an Arrow builder for resources.
type ResourceBuilder struct {
	released bool

	rBuilder *builder.RecordBuilderExt

	builder *builder.StructBuilder // `resource` builder
	ab      *AttributesBuilder     // `attributes` field builder
	dacb    *builder.Uint32Builder // `dropped_attributes_count` field builder
}

// NewResourceBuilder creates a new resource builder with a given allocator.
func NewResourceBuilder(pool memory.Allocator, dictConfig *cfg.DictionaryConfig) *ResourceBuilder {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: constants.Resource, Type: ResourceDT, Metadata: acommon.Metadata(acommon.Optional)},
	}, nil)
	rBuilder := builder.NewRecordBuilderExt(pool, schema, dictConfig)
	return ResourceBuilderFrom(rBuilder.StructBuilder(constants.Resource))
}

// ResourceBuilderFrom creates a new resource builder from an existing struct builder.
func ResourceBuilderFrom(builder *builder.StructBuilder) *ResourceBuilder {
	return &ResourceBuilder{
		released: false,
		builder:  builder,
		ab:       AttributesBuilderFrom(builder.MapBuilder(constants.Attributes)),
		dacb:     builder.Uint32Builder(constants.DroppedAttributesCount),
	}
}

// Append appends a new resource to the builder.
func (b *ResourceBuilder) Append(resource pcommon.Resource) error {
	if b.released {
		return fmt.Errorf("resource builder already released")
	}

	return b.builder.Append(resource, func() error {
		if err := b.ab.Append(resource.Attributes()); err != nil {
			return err
		}
		b.dacb.AppendNonZero(resource.DroppedAttributesCount())
		return nil
	})
}

// Build builds the resource array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ResourceBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("attribute builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the memory allocated by the builder.
func (b *ResourceBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
