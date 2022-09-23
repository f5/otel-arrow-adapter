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

package datagen

import (
	"time"

	"golang.org/x/exp/rand"

	coltracepb "go.opentelemetry.io/collector/pdata/ptrace"
	commonpb "go.opentelemetry.io/collector/pdata/pcommon"
	resourcepb "go.opentelemetry.io/collector/pdata/pcommon"
	tracepb "go.opentelemetry.io/collector/pdata/ptrace"
)

var EVENT_NAMES = []string{"empty", "dns-lookup", "tcp-connect", "tcp-handshake", "tcp-send", "tcp-receive", "tcp-close", "http-send", "http-receive", "http-close", "message-send", "message-receive", "message-close", "grpc-send", "grpc-receive", "grpc-close", "grpc-status", "grpc-trailers", "unknown"}
var TRACE_STATES = []string{"started", "ended", "unknown"}

type TraceGenerator struct {
	resourceAttributes    []pcommon.Map
	defaultSchemaUrl      string
	instrumentationScopes []pcommon.InstrumentationScope
	dataGenerator         *DataGenerator
}

func NewTraceGenerator(resourceAttributes []pcommon.Map, instrumentationScopes []pcommon.InstrumentationScope) *TraceGenerator {
	return &TraceGenerator{
		resourceAttributes:    resourceAttributes,
		defaultSchemaUrl:      "",
		instrumentationScopes: instrumentationScopes,
		dataGenerator:         NewDataGenerator(uint64(time.Now().UnixNano() / int64(time.Millisecond))),
	}
}

func (lg *TraceGenerator) Generate(batchSize int, collectInterval time.Duration) *coltracepb.Traces {
	resourceAttrs := lg.resourceAttributes[rand.Intn(len(lg.resourceAttributes))]
	scopeAttrs := lg.instrumentationScopes[rand.Intn(len(lg.instrumentationScopes))]
	spans := make([]*tracepb.Span, 0, batchSize)

	rand.Seed(uint64(time.Now().UnixNano()))
	for i := 0; i < batchSize; i++ {
		lg.dataGenerator.AdvanceTime(collectInterval)
		spans = append(spans, Spans(lg.dataGenerator)...)
	}

	return &coltracepb.Traces{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes:             resourceAttrs,
					DroppedAttributesCount: 0,
				},
				SchemaUrl: lg.defaultSchemaUrl,
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Scope:     scopeAttrs,
						Spans:     spans,
						SchemaUrl: "",
					},
				},
			},
		},
	}
}

func Spans(dataGenerator *DataGenerator) []*tracepb.Span {
	dataGenerator.NextId8Bits()
	dataGenerator.NextId16Bits()

	traceId := dataGenerator.Id16Bits()
	rootSpanId := dataGenerator.Id8Bits()
	rootStartTime := dataGenerator.CurrentTime()
	rootEndTime := dataGenerator.CurrentTime() + 1 + uint64(rand.Intn(6))

	dataGenerator.AdvanceTime(time.Duration(rand.Intn(10)))

	dataGenerator.NextId8Bits()
	userAccountSpanId := dataGenerator.Id8Bits()
	userAccountStartTime := dataGenerator.CurrentTime()
	userAccountEndTime := dataGenerator.CurrentTime() + uint64(rand.Intn(6))

	dataGenerator.NextId8Bits()
	userPreferencesSpanId := dataGenerator.Id8Bits()
	userPreferenceStartTime := dataGenerator.CurrentTime()
	userPreferenceEndTime := dataGenerator.CurrentTime() + uint64(rand.Intn(4))

	spans := []*tracepb.Span{
		{
			TraceId:                traceId,
			SpanId:                 rootSpanId,
			Name:                   "GET /user-info",
			StartTimeUnixNano:      rootStartTime,
			EndTimeUnixNano:        rootEndTime,
			Kind:                   tracepb.Span_SPAN_KIND_SERVER,
			Attributes:             DefaultAttributes(),
			DroppedAttributesCount: 0,
			Events:                 events(dataGenerator),
			DroppedEventsCount:     0,
			Links:                  links(dataGenerator),
			DroppedLinksCount:      0,
			Status: &tracepb.Status{
				Code:    tracepb.Status_STATUS_CODE_OK,
				Message: "OK",
			},
		},
		{
			TraceId:                traceId,
			SpanId:                 userAccountSpanId,
			Name:                   "user-account",
			StartTimeUnixNano:      userAccountStartTime,
			EndTimeUnixNano:        userAccountEndTime,
			Kind:                   tracepb.Span_SPAN_KIND_SERVER,
			Attributes:             DefaultAttributes(),
			DroppedAttributesCount: 0,
			Events:                 events(dataGenerator),
			DroppedEventsCount:     0,
			Links:                  links(dataGenerator),
			DroppedLinksCount:      0,
			Status: &tracepb.Status{
				Code:    tracepb.Status_STATUS_CODE_OK,
				Message: "OK",
			},
		},
		{
			TraceId:                traceId,
			SpanId:                 userPreferencesSpanId,
			Name:                   "user-preferences",
			StartTimeUnixNano:      userPreferenceStartTime,
			EndTimeUnixNano:        userPreferenceEndTime,
			Kind:                   tracepb.Span_SPAN_KIND_SERVER,
			Attributes:             DefaultAttributes(),
			DroppedAttributesCount: 0,
			Events:                 events(dataGenerator),
			DroppedEventsCount:     0,
			Links:                  links(dataGenerator),
			DroppedLinksCount:      0,
			Status: &tracepb.Status{
				Code:    tracepb.Status_STATUS_CODE_OK,
				Message: "OK",
			},
		},
	}

	rand.Shuffle(len(spans), func(i, j int) { spans[i], spans[j] = spans[j], spans[i] })

	return spans
}

// events returns a slice of events for the span.
func events(dataGenerator *DataGenerator) []*tracepb.Span_Event {
	eventCount := rand.Intn(8) + 2
	events := make([]*tracepb.Span_Event, eventCount)
	for i := 0; i < eventCount; i++ {
		name := EVENT_NAMES[rand.Intn(len(EVENT_NAMES))]
		attributes := DefaultSpanEventAttributes()
		if name == "empty" {
			attributes = nil
		}
		events[i] = &tracepb.Span_Event{
			TimeUnixNano:           dataGenerator.CurrentTime() + uint64(rand.Intn(5)),
			Name:                   name,
			Attributes:             attributes,
			DroppedAttributesCount: 0,
		}
	}
	return events
}

// links returns a slice of links for the span.
func links(dataGenerator *DataGenerator) []*tracepb.Span_Link {
	linkCount := rand.Intn(8) + 2
	dataGenerator.NextId16Bits()

	links := make([]*tracepb.Span_Link, linkCount)
	for i := 0; i < linkCount; i++ {
		dataGenerator.NextId8Bits()
		links[i] = &tracepb.Span_Link{
			TraceId:                dataGenerator.Id16Bits(),
			SpanId:                 dataGenerator.Id8Bits(),
			TraceState:             TRACE_STATES[rand.Intn(len(TRACE_STATES))],
			Attributes:             DefaultSpanLinkAttributes(),
			DroppedAttributesCount: 0,
		}
	}
	return links
}
