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
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// LinkDT is the Arrow Data Type describing a link event.
var (
	LinkDT = arrow.StructOf([]arrow.Field{
		{Name: constants.TraceId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.SpanId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.TraceState, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.Attributes, Type: acommon.AttributesDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.DroppedAttributesCount, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
	}...)
)

type LinkBuilder struct {
	released bool

	builder *builder.StructBuilder

	tib  *builder.FixedSizeBinaryBuilder // `trace_id` builder
	sib  *builder.FixedSizeBinaryBuilder // `span_id` builder
	tsb  *builder.StringBuilder          // `trace_state` builder
	ab   *acommon.AttributesBuilder      // `attributes` builder
	dacb *builder.Uint32Builder          // `dropped_attributes_count` builder
}

func LinkBuilderFrom(lb *builder.StructBuilder) *LinkBuilder {
	return &LinkBuilder{
		released: false,
		builder:  lb,
		tib:      lb.FixedSizeBinaryBuilder(constants.TraceId),
		sib:      lb.FixedSizeBinaryBuilder(constants.SpanId),
		tsb:      lb.StringBuilder(constants.TraceState),
		ab:       acommon.AttributesBuilderFrom(lb.MapBuilder(constants.Attributes)),
		dacb:     lb.Uint32Builder(constants.DroppedAttributesCount),
	}
}

// Append appends a new link to the builder.
func (b *LinkBuilder) Append(link ptrace.SpanLink, sharedAttributes *common.SharedAttributes) error {
	if b.released {
		return werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	return b.builder.Append(link, func() error {
		tid := link.TraceID()
		b.tib.Append(tid[:])
		sid := link.SpanID()
		b.sib.Append(sid[:])
		b.tsb.AppendNonEmpty(link.TraceState().AsRaw())
		b.dacb.AppendNonZero(link.DroppedAttributesCount())
		return b.ab.AppendUniqueAttributes(link.Attributes(), sharedAttributes, nil)
	})
}

// Build builds the link array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *LinkBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the memory allocated by the builder.
func (b *LinkBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
