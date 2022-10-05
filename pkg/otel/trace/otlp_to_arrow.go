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

package trace

import (
	"strings"

	"github.com/apache/arrow/go/v9/arrow"

	"otel-arrow-adapter/pkg/air"
	"otel-arrow-adapter/pkg/air/config"
	"otel-arrow-adapter/pkg/air/rfield"
	"otel-arrow-adapter/pkg/otel/common"
	"otel-arrow-adapter/pkg/otel/constants"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// OtlpTraceToArrowRecords converts an OTLP trace to one or more Arrow records.
// TODO: add a reference to the OTEP 0156 section that describes this mapping.
func OtlpTraceToArrowRecords(rr *air.RecordRepository, request ptrace.Traces, cfg *config.Config) ([]arrow.Record, error) {
	resSpansPerSig := make(map[string][]rfield.Value)

	for i := 0; i < request.ResourceSpans().Len(); i++ {
		resourceSpans := request.ResourceSpans().At(i)

		// Resource fields that are common to all spans in the resource
		commonResFields := make([]*rfield.Field, 0, 3)

		// Add resource fields (attributes and dropped attributes count)
		commonResFields = append(commonResFields, common.ResourceField(resourceSpans.Resource(), cfg))

		// Add schema URL
		if resourceSpans.SchemaUrl() != "" {
			commonResFields = append(commonResFields, rfield.NewStringField(constants.SCHEMA_URL, resourceSpans.SchemaUrl()))
		}

		// Group scope spans per signature
		scopeSpansPerSig := groupScopeSpansPerSig(resourceSpans, cfg)

		// All scope spans sharing the same signature are represented as an AIR struct containing the common resource
		// fields and a list of scope spans.
		for sig, scopeSpans := range scopeSpansPerSig {
			resFieldsCopy := make([]*rfield.Field, 0, len(commonResFields)+1)
			copy(resFieldsCopy, commonResFields)
			resFieldsCopy = append(resFieldsCopy, rfield.NewListField(constants.SCOPE_SPANS, rfield.List{Values: scopeSpans}))
			resSpansPerSig[sig] = append(resSpansPerSig[sig], rfield.NewStruct(resFieldsCopy))
		}
	}

	// All resource spans sharing the same signature are represented as an AIR record.
	for _, resSpans := range resSpansPerSig {
		record := air.NewRecord()
		record.ListField(constants.RESOURCE_SPANS, rfield.List{Values: resSpans})
		rr.AddRecord(record)
	}

	// Build all Arrow records from the AIR records
	records, err := rr.BuildRecords()
	if err != nil {
		return nil, err
	}

	return records, nil
}

// groupScopeSpansPerSig groups spans per signature.
func groupScopeSpansPerSig(resourceSpans ptrace.ResourceSpans, cfg *config.Config) (scopeSpansPerSig map[string][]rfield.Value) {
	scopeSpansPerSig = make(map[string][]rfield.Value)
	for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
		scopeSpans := resourceSpans.ScopeSpans().At(j)
		fields := make([]*rfield.Field, 0, 3)
		fields = append(fields, common.ScopeField(constants.SCOPE_SPANS, scopeSpans.Scope(), cfg))
		if scopeSpans.SchemaUrl() != "" {
			fields = append(fields, rfield.NewStringField(constants.SCHEMA_URL, scopeSpans.SchemaUrl()))
		}

		spansPerSig := groupSpansPerSig(scopeSpans, cfg)
		for sig, spans := range spansPerSig {
			scopeSpansFields := make([]*rfield.Field, 0, 3)
			copy(scopeSpansFields, fields)
			scopeSpansFields = append(scopeSpansFields, rfield.NewListField(constants.SPANS, rfield.List{
				Values: spans,
			}))
			scopeSpansPerSig[sig] = append(scopeSpansPerSig[sig], rfield.NewStruct(scopeSpansFields))
		}
	}
	return
}

// groupSpansPerSig groups spans per signature.
func groupSpansPerSig(scopeSpans ptrace.ScopeSpans, cfg *config.Config) (spansPerSig map[string][]rfield.Value) {
	spansPerSig = make(map[string][]rfield.Value)
	for k := 0; k < scopeSpans.Spans().Len(); k++ {
		span := scopeSpans.Spans().At(k)

		fields := make([]*rfield.Field, 0, 10)
		if ts := span.StartTimestamp(); ts > 0 {
			fields = append(fields, rfield.NewU64Field(constants.START_TIME_UNIX_NANO, uint64(ts)))
		}
		if ts := span.EndTimestamp(); ts > 0 {
			fields = append(fields, rfield.NewU64Field(constants.END_TIME_UNIX_NANO, uint64(ts)))
		}

		if tid := span.TraceID(); !tid.IsEmpty() {
			fields = append(fields, rfield.NewBinaryField(constants.TRACE_ID, tid[:]))
		}
		if sid := span.SpanID(); !sid.IsEmpty() {
			fields = append(fields, rfield.NewBinaryField(constants.SPAN_ID, sid[:]))
		}
		if span.TraceState() != "" {
			fields = append(fields, rfield.NewStringField(constants.TRACE_STATE, string(span.TraceState())))
		}
		if psid := span.ParentSpanID(); !psid.IsEmpty() {
			fields = append(fields, rfield.NewBinaryField(constants.PARENT_SPAN_ID, psid[:]))
		}
		if span.Name() != "" {
			fields = append(fields, rfield.NewStringField(constants.NAME, span.Name()))
		}
		fields = append(fields, rfield.NewI32Field(constants.KIND, int32(span.Kind())))
		attributes := common.NewAttributes(span.Attributes(), cfg)
		if attributes != nil {
			fields = append(fields, attributes)
		}

		if dc := span.DroppedAttributesCount(); dc > 0 {
			fields = append(fields, rfield.NewU32Field(constants.DROPPED_ATTRIBUTES_COUNT, uint32(dc)))
		}

		// Events
		eventsField := events(span.Events(), cfg)
		if eventsField != nil {
			fields = append(fields, eventsField)
		}
		if dc := span.DroppedEventsCount(); dc > 0 {
			fields = append(fields, rfield.NewU32Field(constants.DROPPED_EVENTS_COUNT, uint32(dc)))
		}

		// Links
		linksField := links(span.Links(), cfg)
		if linksField != nil {
			fields = append(fields, linksField)
		}
		if dc := span.DroppedLinksCount(); dc > 0 {
			fields = append(fields, rfield.NewU32Field(constants.DROPPED_LINKS_COUNT, uint32(dc)))
		}

		// Status
		if span.Status().Code() != 0 {
			fields = append(fields, rfield.NewI32Field(constants.STATUS, int32(span.Status().Code())))
		}
		if span.Status().Message() != "" {
			fields = append(fields, rfield.NewStringField(constants.STATUS_MESSAGE, span.Status().Message()))
		}

		spanValue := rfield.NewStruct(fields)

		// Compute signature for the span value
		var sig strings.Builder
		attributes.Normalize()
		attributes.WriteSignature(&sig)
		//spanValue.Normalize()
		//spanValue.WriteSignature(&sig)
		spansPerSig[sig.String()] = append(spansPerSig[sig.String()], spanValue)
	}
	return
}

// events converts OTLP span events to the AIR representation.
func events(events ptrace.SpanEventSlice, cfg *config.Config) *rfield.Field {
	if events.Len() == 0 {
		return nil
	}

	convertedEvents := make([]rfield.Value, 0, events.Len())

	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		fields := make([]*rfield.Field, 0, 4)

		if ts := event.Timestamp(); ts > 0 {
			fields = append(fields, rfield.NewU64Field(constants.TIME_UNIX_NANO, uint64(ts)))
		}
		if event.Name() != "" {
			fields = append(fields, rfield.NewStringField(constants.NAME, event.Name()))
		}
		attributes := common.NewAttributes(event.Attributes(), cfg)
		if attributes != nil {
			fields = append(fields, attributes)
		}
		if dc := event.DroppedAttributesCount(); dc > 0 {
			fields = append(fields, rfield.NewU32Field(constants.DROPPED_ATTRIBUTES_COUNT, uint32(dc)))
		}
		convertedEvents = append(convertedEvents, &rfield.Struct{
			Fields: fields,
		})
	}
	return rfield.NewListField(constants.SPAN_EVENTS, rfield.List{
		Values: convertedEvents,
	})
}

// links converts OTLP span links to the AIR representation.
func links(links ptrace.SpanLinkSlice, cfg *config.Config) *rfield.Field {
	if links.Len() == 0 {
		return nil
	}

	convertedLinks := make([]rfield.Value, 0, links.Len())

	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		fields := make([]*rfield.Field, 0, 4)

		if tid := link.TraceID(); !tid.IsEmpty() {
			fields = append(fields, rfield.NewBinaryField(constants.TRACE_ID, tid[:]))
		}
		if sid := link.SpanID(); !sid.IsEmpty() {
			fields = append(fields, rfield.NewBinaryField(constants.SPAN_ID, sid[:]))
		}
		if link.TraceState() != "" {
			fields = append(fields, rfield.NewStringField(constants.TRACE_STATE, string(link.TraceState())))
		}
		attributes := common.NewAttributes(link.Attributes(), cfg)
		if attributes != nil {
			fields = append(fields, attributes)
		}
		if dc := link.DroppedAttributesCount(); dc > 0 {
			fields = append(fields, rfield.NewU32Field(constants.DROPPED_ATTRIBUTES_COUNT, uint32(dc)))
		}
		convertedLinks = append(convertedLinks, &rfield.Struct{
			Fields: fields,
		})
	}
	return rfield.NewListField(constants.SPAN_LINKS, rfield.List{
		Values: convertedLinks,
	})
}
