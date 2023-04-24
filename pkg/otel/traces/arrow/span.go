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

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// SpanDT is the Arrow Data Type describing a span.
var (
	SpanDT = arrow.StructOf([]arrow.Field{
		{Name: constants.StartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: constants.DurationTimeUnixNano, Type: arrow.FixedWidthTypes.Duration_ms, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.TraceId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}},
		{Name: constants.SpanId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}},
		{Name: constants.TraceState, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.ParentSpanId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Name, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.KIND, Type: arrow.PrimitiveTypes.Int32, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.AttributesID, Type: arrow.PrimitiveTypes.Uint16, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding)},
		{Name: constants.DroppedAttributesCount, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.EventsID, Type: arrow.PrimitiveTypes.Uint16, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding)},
		{Name: constants.DroppedEventsCount, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.LinksID, Type: arrow.PrimitiveTypes.Uint16, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding)},
		{Name: constants.DroppedLinksCount, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Status, Type: StatusDT, Metadata: schema.Metadata(schema.Optional)},
	}...)
)

// SpanBuilder is a helper to build a span.
type SpanBuilder struct {
	released bool

	builder *builder.StructBuilder

	stunb *builder.TimestampBuilder       // start time unix nano builder
	dtunb *builder.DurationBuilder        // duration time unix nano builder
	tib   *builder.FixedSizeBinaryBuilder // trace id builder
	sib   *builder.FixedSizeBinaryBuilder // span id builder
	tsb   *builder.StringBuilder          // trace state builder
	psib  *builder.FixedSizeBinaryBuilder // parent span id builder
	nb    *builder.StringBuilder          // name builder
	kb    *builder.Int32Builder           // kind builder
	aib   *builder.Uint16DeltaBuilder     // attributes id builder
	dacb  *builder.Uint32Builder          // dropped attributes count builder
	seib  *builder.Uint16DeltaBuilder     // span events id builder
	decb  *builder.Uint32Builder          // dropped events count builder
	slib  *builder.Uint16DeltaBuilder     // span links id builder
	dlcb  *builder.Uint32Builder          // dropped links count builder
	sb    *StatusBuilder                  // status builder
}

func SpanBuilderFrom(sb *builder.StructBuilder) *SpanBuilder {
	aib := sb.Uint16DeltaBuilder(constants.AttributesID)
	// As the attributes are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	aib.SetMaxDelta(1)

	seib := sb.Uint16DeltaBuilder(constants.EventsID)
	// As the events are sorted before insertion, the delta between two
	// consecutive events ID should always be <=1.
	seib.SetMaxDelta(1)

	slib := sb.Uint16DeltaBuilder(constants.LinksID)
	// As the events are sorted before insertion, the delta between two
	// consecutive events ID should always be <=1.
	slib.SetMaxDelta(1)

	return &SpanBuilder{
		released: false,
		builder:  sb,
		stunb:    sb.TimestampBuilder(constants.StartTimeUnixNano),
		dtunb:    sb.DurationBuilder(constants.DurationTimeUnixNano),
		tib:      sb.FixedSizeBinaryBuilder(constants.TraceId),
		sib:      sb.FixedSizeBinaryBuilder(constants.SpanId),
		tsb:      sb.StringBuilder(constants.TraceState),
		psib:     sb.FixedSizeBinaryBuilder(constants.ParentSpanId),
		nb:       sb.StringBuilder(constants.Name),
		kb:       sb.Int32Builder(constants.KIND),
		aib:      aib,
		dacb:     sb.Uint32Builder(constants.DroppedAttributesCount),
		seib:     seib,
		decb:     sb.Uint32Builder(constants.DroppedEventsCount),
		slib:     slib,
		dlcb:     sb.Uint32Builder(constants.DroppedLinksCount),
		sb:       StatusBuilderFrom(sb.StructBuilder(constants.Status)),
	}
}

// Build builds the span array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *SpanBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Append appends a new span to the builder.
func (b *SpanBuilder) Append(span *ptrace.Span, sharedData *SharedData, relatedData *RelatedData) error {
	if b.released {
		return werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	return b.builder.Append(span, func() error {
		b.stunb.Append(arrow.Timestamp(span.StartTimestamp()))
		duration := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Nanoseconds()
		b.dtunb.Append(arrow.Duration(duration))
		tib := span.TraceID()
		b.tib.Append(tib[:])
		sib := span.SpanID()
		b.sib.Append(sib[:])
		b.tsb.AppendNonEmpty(span.TraceState().AsRaw())
		psib := span.ParentSpanID()
		b.psib.Append(psib[:])
		b.nb.AppendNonEmpty(span.Name())
		b.kb.AppendNonZero(int32(span.Kind()))

		// Span Attributes
		ID, err := relatedData.AttrsBuilders().Span().Accumulator().AppendUniqueAttributes(span.Attributes(), sharedData.sharedAttributes, nil)
		if err != nil {
			return werror.Wrap(err)
		}
		if ID >= 0 {
			b.aib.Append(uint16(ID))
		} else {
			b.aib.AppendNull()
		}
		b.dacb.AppendNonZero(span.DroppedAttributesCount())

		// Events
		ID, err = relatedData.EventBuilder().Accumulator().Append(span.Events())
		if err != nil {
			return werror.Wrap(err)
		}
		if ID >= 0 {
			b.seib.Append(uint16(ID))
		} else {
			b.seib.AppendNull()
		}
		b.decb.AppendNonZero(span.DroppedEventsCount())

		// Links
		ID, err = relatedData.LinkBuilder().Accumulator().Append(span.Links())
		if err != nil {
			return werror.Wrap(err)
		}
		if ID >= 0 {
			b.slib.Append(uint16(ID))
		} else {
			b.slib.AppendNull()
		}
		b.dlcb.AppendNonZero(span.DroppedLinksCount())

		return b.sb.Append(span.Status())
	})
}

// Release releases the memory allocated by the builder.
func (b *SpanBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
