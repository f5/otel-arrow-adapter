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

package traces

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

type resourceSpanFields struct {
	resource   *rfield.Field
	scopeSpans []rfield.Value
	schemaUrl  *rfield.Field
}

type scopeSpanFields struct {
	scope     *rfield.Field
	spans     []rfield.Value
	schemaUrl *rfield.Field
}

func (r *resourceSpanFields) resourceSpan() rfield.Value {
	fields := make([]*rfield.Field, 0, 3)
	if r.resource != nil {
		fields = append(fields, r.resource)
	}
	if r.schemaUrl != nil {
		fields = append(fields, r.schemaUrl)
	}
	if len(r.scopeSpans) > 0 {
		fields = append(fields, rfield.NewListField(constants.SCOPE_SPANS, rfield.List{Values: r.scopeSpans}))
	}
	return rfield.NewStruct(fields)
}

func (s *scopeSpanFields) scopeSpan() rfield.Value {
	fields := make([]*rfield.Field, 0, 3)
	if s.scope != nil {
		fields = append(fields, s.scope)
	}
	if s.schemaUrl != nil {
		fields = append(fields, s.schemaUrl)
	}
	if len(s.spans) > 0 {
		fields = append(fields, rfield.NewListField(constants.SPANS, rfield.List{Values: s.spans}))
	}

	return rfield.NewStruct(fields)
}

// OtlpTracesToArrowRecords converts an OTLP traces to one or more Arrow records.
// TODO: add a reference to the OTEP 0156 section that describes this mapping.
func OtlpTracesToArrowRecords(rr *air.RecordRepository, request ptrace.Traces, cfg *config.Config) ([]arrow.Record, error) {
	// Map used to merge all scope spans sharing the same resource span signature.
	// A resource span signature is based on the resource attributes, the dropped attributes count and the schema URL
	resSpansPerSig := make(map[string]*resourceSpanFields)

	for i := 0; i < request.ResourceSpans().Len(); i++ {
		resourceSpans := request.ResourceSpans().At(i)

		// Add resource fields (attributes and dropped attributes count)
		resField, resSig := common.ResourceFieldWithSig(resourceSpans.Resource(), cfg)

		// Add schema URL
		var schemaUrl *rfield.Field
		if resourceSpans.SchemaUrl() != "" {
			schemaUrl = rfield.NewStringField(constants.SCHEMA_URL, resourceSpans.SchemaUrl())
			resSig += ",schema_url:" + resourceSpans.SchemaUrl()
		}

		// Create a new entry in the map if the signature is not already present
		resSpanFields := resSpansPerSig[resSig]
		if resSpanFields == nil {
			resSpanFields = &resourceSpanFields{
				resource:   resField,
				scopeSpans: make([]rfield.Value, 0, 16),
				schemaUrl:  schemaUrl,
			}
			resSpansPerSig[resSig] = resSpanFields
		}

		// Group scope spans per signature
		scopeSpansPerSig := groupScopeSpansPerSig(resourceSpans, cfg)

		// TODO Merge spans sharing the same resource spans and scope spans signatures.

		// Add scope spans
		for _, scopeSpans := range scopeSpansPerSig {
			resSpanFields.scopeSpans = append(resSpanFields.scopeSpans, scopeSpans.scopeSpan())
		}
	}

	// TODO Other way to explore -> create a single Arrow record from a list of resource spans.
	// All resource spans sharing the same signature are represented as an AIR record.
	for _, resSpanFields := range resSpansPerSig {
		record := air.NewRecord()
		record.ListField(constants.RESOURCE_SPANS, rfield.List{Values: []rfield.Value{
			resSpanFields.resourceSpan(),
		}})
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
// A scope span signature is based on the scope attributes, the dropped attributes count and the schema URL.
func groupScopeSpansPerSig(resourceSpans ptrace.ResourceSpans, cfg *config.Config) (scopeSpansPerSig map[string]*scopeSpanFields) {
	scopeSpansPerSig = make(map[string]*scopeSpanFields)
	for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
		scopeSpans := resourceSpans.ScopeSpans().At(j)

		var sig strings.Builder

		scopeField := common.ScopeField(constants.SCOPE, scopeSpans.Scope(), cfg)
		scopeField.Normalize()
		scopeField.WriteSignature(&sig)

		var schemaField *rfield.Field
		if scopeSpans.SchemaUrl() != "" {
			schemaField = rfield.NewStringField(constants.SCHEMA_URL, scopeSpans.SchemaUrl())
			sig.WriteString(",")
			schemaField.WriteSignature(&sig)
		}

		// Create a new entry in the map if the signature is not already present
		ssSig := sig.String()
		ssFields := scopeSpansPerSig[ssSig]
		if ssFields == nil {
			ssFields = &scopeSpanFields{
				scope:     scopeField,
				spans:     make([]rfield.Value, 0, 16),
				schemaUrl: schemaField,
			}
			scopeSpansPerSig[ssSig] = ssFields
		}

		spans := spans(scopeSpans, cfg)
		ssFields.spans = append(ssFields.spans, spans...)
	}
	return
}

// spans converts ptrace.ScopeSpans in their AIR representation.
func spans(scopeSpans ptrace.ScopeSpans, cfg *config.Config) (spans []rfield.Value) {
	spans = make([]rfield.Value, 0, scopeSpans.Spans().Len())
	for k := 0; k < scopeSpans.Spans().Len(); k++ {
		span := scopeSpans.Spans().At(k)

		fields := make([]*rfield.Field, 0, 15)
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
		statusField := status(span)
		if statusField != nil {
			fields = append(fields, rfield.NewStructField(constants.STATUS, *statusField))
		}

		spanValue := rfield.NewStruct(fields)

		spans = append(spans, spanValue)
	}
	return
}

// status converts OTLP span status to their AIR representation or returns nil when the status has no field.
func status(span ptrace.Span) *rfield.Struct {
	fields := make([]*rfield.Field, 0, 2)

	if span.Status().Code() != 0 {
		fields = append(fields, rfield.NewI32Field(constants.STATUS, int32(span.Status().Code())))
	}
	if span.Status().Message() != "" {
		fields = append(fields, rfield.NewStringField(constants.STATUS_MESSAGE, span.Status().Message()))
	}

	if len(fields) > 0 {
		return rfield.NewStruct(fields)
	} else {
		return nil
	}
}

// events converts OTLP span events into their AIR representation or returns nil when there is no events.
func events(events ptrace.SpanEventSlice, cfg *config.Config) *rfield.Field {
	if events.Len() == 0 {
		return nil
	}

	airEvents := make([]rfield.Value, 0, events.Len())

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
		airEvents = append(airEvents, &rfield.Struct{
			Fields: fields,
		})
	}
	return rfield.NewListField(constants.SPAN_EVENTS, rfield.List{
		Values: airEvents,
	})
}

// links converts OTLP span links into their AIR representation or returns nil when there is no links.
func links(links ptrace.SpanLinkSlice, cfg *config.Config) *rfield.Field {
	if links.Len() == 0 {
		return nil
	}

	airLinks := make([]rfield.Value, 0, links.Len())

	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		fields := make([]*rfield.Field, 0, 5)

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
		airLinks = append(airLinks, &rfield.Struct{
			Fields: fields,
		})
	}
	return rfield.NewListField(constants.SPAN_LINKS, rfield.List{
		Values: airLinks,
	})
}
