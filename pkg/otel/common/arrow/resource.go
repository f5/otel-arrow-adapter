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

package arrow

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// ResourceDT is the Arrow Data Type describing a resource.
var (
	ResourceDT = arrow.StructOf([]arrow.Field{
		{
			Name:     constants.AttributesID,
			Type:     arrow.PrimitiveTypes.Uint32,
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

	builder *builder.StructBuilder      // `resource` builder
	aib     *builder.Uint32DeltaBuilder // attributes id builder
	dacb    *builder.Uint32Builder      // `dropped_attributes_count` field builder
}

// NewResourceBuilder creates a new resource builder with a given allocator.
func NewResourceBuilder(builder *builder.StructBuilder) *ResourceBuilder {
	return ResourceBuilderFrom(builder)
}

// ResourceBuilderFrom creates a new resource builder from an existing struct builder.
func ResourceBuilderFrom(builder *builder.StructBuilder) *ResourceBuilder {
	return &ResourceBuilder{
		released: false,
		builder:  builder,
		aib:      builder.Uint32DeltaBuilder(constants.AttributesID),
		dacb:     builder.Uint32Builder(constants.DroppedAttributesCount),
	}
}

// Append appends a new resource to the builder.
func (b *ResourceBuilder) Append(resource *pcommon.Resource, attrsCollector *AttributesCollector) error {
	if b.released {
		return werror.Wrap(ErrBuilderAlreadyReleased)
	}

	return b.builder.Append(resource, func() error {
		ID, err := attrsCollector.Append(resource.Attributes())
		if err != nil {
			return werror.Wrap(err)
		}
		if ID >= 0 {
			b.aib.Append(uint32(ID))
		} else {
			b.aib.AppendNull()
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
		return nil, werror.Wrap(ErrBuilderAlreadyReleased)
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
