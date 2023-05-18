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
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrow2 "github.com/f5/otel-arrow-adapter/pkg/arrow"
	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/otel/stats"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// Schema is the Arrow schema for the OTLP Arrow Traces record.
var (
	TracesSchema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint16, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding)},
		{Name: constants.Resource, Type: acommon.ResourceDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SchemaUrl, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.Scope, Type: acommon.ScopeDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: "scope_schema_url", Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.StartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: constants.DurationTimeUnixNano, Type: arrow.FixedWidthTypes.Duration_ms, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.TraceId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}},
		{Name: constants.SpanId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}},
		{Name: constants.TraceState, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.ParentSpanId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Name, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.KIND, Type: arrow.PrimitiveTypes.Int32, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.DroppedAttributesCount, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.DroppedEventsCount, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.DroppedLinksCount, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Status, Type: StatusDT, Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

// TracesBuilder is a helper to build a list of resource spans.
type TracesBuilder struct {
	released bool

	builder *builder.RecordBuilderExt // Record builder

	rb    *acommon.ResourceBuilder        // `resource` builder
	rschb *builder.StringBuilder          // resource `schema_url` builder
	scb   *acommon.ScopeBuilder           // `scope` builder
	sschb *builder.StringBuilder          // scope `schema_url` builder
	ib    *builder.Uint16DeltaBuilder     //  id builder
	stunb *builder.TimestampBuilder       // start time unix nano builder
	dtunb *builder.DurationBuilder        // duration time unix nano builder
	tib   *builder.FixedSizeBinaryBuilder // trace id builder
	sib   *builder.FixedSizeBinaryBuilder // span id builder
	tsb   *builder.StringBuilder          // trace state builder
	psib  *builder.FixedSizeBinaryBuilder // parent span id builder
	nb    *builder.StringBuilder          // name builder
	kb    *builder.Int32Builder           // kind builder
	dacb  *builder.Uint32Builder          // dropped attributes count builder
	decb  *builder.Uint32Builder          // dropped events count builder
	dlcb  *builder.Uint32Builder          // dropped links count builder
	sb    *StatusBuilder                  // status builder

	optimizer *TracesOptimizer
	analyzer  *TracesAnalyzer

	relatedData *RelatedData
}

// NewTracesBuilder creates a new TracesBuilder with a given allocator.
func NewTracesBuilder(
	rBuilder *builder.RecordBuilderExt,
	cfg *Config,
	stats *stats.ProducerStats,
) (*TracesBuilder, error) {
	var optimizer *TracesOptimizer
	var analyzer *TracesAnalyzer

	relatedData, err := NewRelatedData(cfg, stats)
	if err != nil {
		panic(err)
	}

	if stats.SchemaStatsEnabled {
		optimizer = NewTracesOptimizer(cfg.Span.Sorter)
		analyzer = NewTraceAnalyzer()
	} else {
		optimizer = NewTracesOptimizer(cfg.Span.Sorter)
	}

	b := &TracesBuilder{
		released:    false,
		builder:     rBuilder,
		optimizer:   optimizer,
		analyzer:    analyzer,
		relatedData: relatedData,
	}

	if err := b.init(); err != nil {
		return nil, werror.Wrap(err)
	}

	return b, nil
}

func (b *TracesBuilder) init() error {
	ib := b.builder.Uint16DeltaBuilder(constants.ID)
	// As the attributes are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	ib.SetMaxDelta(1)

	b.ib = ib
	b.rb = acommon.ResourceBuilderFrom(b.builder.StructBuilder(constants.Resource))
	b.rschb = b.builder.StringBuilder(constants.SchemaUrl)
	b.scb = acommon.ScopeBuilderFrom(b.builder.StructBuilder(constants.Scope))
	b.sschb = b.builder.StringBuilder(constants.SchemaUrl)

	b.stunb = b.builder.TimestampBuilder(constants.StartTimeUnixNano)
	b.dtunb = b.builder.DurationBuilder(constants.DurationTimeUnixNano)
	b.tib = b.builder.FixedSizeBinaryBuilder(constants.TraceId)
	b.sib = b.builder.FixedSizeBinaryBuilder(constants.SpanId)
	b.tsb = b.builder.StringBuilder(constants.TraceState)
	b.psib = b.builder.FixedSizeBinaryBuilder(constants.ParentSpanId)
	b.nb = b.builder.StringBuilder(constants.Name)
	b.kb = b.builder.Int32Builder(constants.KIND)
	b.dacb = b.builder.Uint32Builder(constants.DroppedAttributesCount)
	b.decb = b.builder.Uint32Builder(constants.DroppedEventsCount)
	b.dlcb = b.builder.Uint32Builder(constants.DroppedLinksCount)
	b.sb = StatusBuilderFrom(b.builder.StructBuilder(constants.Status))

	return nil
}

func (b *TracesBuilder) RelatedData() *RelatedData {
	return b.relatedData
}

// Build builds an Arrow Record from the builder.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the record.
//
// This method returns a DictionaryOverflowError if the cardinality of a dictionary
// (or several) exceeds the maximum allowed value.
func (b *TracesBuilder) Build() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		initErr := b.init()
		if initErr != nil {
			err = werror.Wrap(initErr)
		}
	}

	// ToDo TMP
	if err == nil && count == 0 {
		println("Traces")
		arrow2.PrintRecord(record)
		count = count + 1
	}

	return
}

// ToDo TMP
var count = 0

// Append appends a new set of resource spans to the builder.
func (b *TracesBuilder) Append(traces ptrace.Traces) error {
	if b.released {
		return werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	optimTraces := b.optimizer.Optimize(traces)
	if b.analyzer != nil {
		b.analyzer.Analyze(optimTraces)
		b.analyzer.ShowStats("")
	}

	spanID := uint16(0)
	var resSpanID, scopeSpanID string
	var resID, scopeID int64
	var err error

	attrsAccu := b.relatedData.AttrsBuilders().Span().Accumulator()
	eventsAccu := b.relatedData.EventBuilder().Accumulator()
	linksAccu := b.relatedData.LinkBuilder().Accumulator()

	for _, span := range optimTraces.Spans {
		spanAttrs := span.Span.Attributes()
		spanEvents := span.Span.Events()
		spanLinks := span.Span.Links()

		ID := spanID

		if spanAttrs.Len() == 0 && spanEvents.Len() == 0 && spanLinks.Len() == 0 {
			// No related data found
			b.ib.AppendNull()
		} else {
			b.ib.Append(ID)
			spanID++
		}

		// Resource spans
		if resSpanID != span.ResourceSpanID {
			resSpanID = span.ResourceSpanID
			resID, err = b.relatedData.AttrsBuilders().Resource().Accumulator().Append(span.Resource.Attributes())
			if err != nil {
				return werror.Wrap(err)
			}
		}
		if err = b.rb.AppendWithAttrsID(resID, span.Resource); err != nil {
			return werror.Wrap(err)
		}
		b.rschb.AppendNonEmpty(span.ResourceSchemaUrl)

		// Scope spans
		if scopeSpanID != span.ScopeSpanID {
			scopeSpanID = span.ScopeSpanID
			scopeID, err = b.relatedData.AttrsBuilders().scope.Accumulator().Append(span.Scope.Attributes())
			if err != nil {
				return werror.Wrap(err)
			}
		}
		if err = b.scb.AppendWithAttrsID(scopeID, span.Scope); err != nil {
			return werror.Wrap(err)
		}
		b.sschb.AppendNonEmpty(span.ScopeSchemaUrl)

		b.stunb.Append(arrow.Timestamp(span.Span.StartTimestamp()))
		duration := span.Span.EndTimestamp().AsTime().Sub(span.Span.StartTimestamp().AsTime()).Nanoseconds()
		b.dtunb.Append(arrow.Duration(duration))
		tib := span.Span.TraceID()
		b.tib.Append(tib[:])
		sib := span.Span.SpanID()
		b.sib.Append(sib[:])
		b.tsb.AppendNonEmpty(span.Span.TraceState().AsRaw())
		psib := span.Span.ParentSpanID()
		b.psib.Append(psib[:])
		b.nb.AppendNonEmpty(span.Span.Name())
		b.kb.AppendNonZero(int32(span.Span.Kind()))

		// Span Attributes
		if spanAttrs.Len() > 0 {
			err = attrsAccu.AppendWithID(ID, spanAttrs)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		b.dacb.AppendNonZero(span.Span.DroppedAttributesCount())

		// Events
		if spanEvents.Len() > 0 {
			err = eventsAccu.Append(ID, spanEvents, nil)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		b.decb.AppendNonZero(span.Span.DroppedEventsCount())

		// Links
		if spanLinks.Len() > 0 {
			err = linksAccu.Append(ID, spanLinks, nil)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		b.dlcb.AppendNonZero(span.Span.DroppedLinksCount())

		if err = b.sb.Append(span.Span.Status()); err != nil {
			return werror.Wrap(err)
		}
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *TracesBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.released = true

		b.relatedData.Release()
	}
}

func (b *TracesBuilder) ShowSchema() {
	b.builder.ShowSchema()
}
