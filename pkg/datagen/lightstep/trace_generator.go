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

package lightstep

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep/topology"
)

type TraceGenerator struct {
	topology       *topology.Topology
	service        string
	route          string
	sequenceNumber int
	random         *rand.Rand
	sync.Mutex
}

func NewTraceGenerator(t *topology.Topology, randSeed *rand.Rand, service string, route string) *TraceGenerator {
	tg := &TraceGenerator{
		topology: t,
		random:   randSeed,
		service:  service,
		route:    route,
	}
	return tg
}

func (g *TraceGenerator) genTraceId() pcommon.TraceID {
	g.Lock()
	defer g.Unlock()
	traceIdBytes := make([]byte, 16)
	g.random.Read(traceIdBytes)
	var traceId [16]byte
	copy(traceId[:], traceIdBytes)
	return traceId
}

func (g *TraceGenerator) genSpanId() pcommon.SpanID {
	g.Lock()
	defer g.Unlock()
	spanIdBytes := make([]byte, 16)
	g.random.Read(spanIdBytes)
	var spanId [8]byte
	copy(spanId[:], spanIdBytes)
	return spanId
}

func (g *TraceGenerator) Generate(startTimeNanos int64) *ptrace.Traces {
	traces := ptrace.NewTraces()

	g.createSpanForServiceRouteCall(&traces, g.service, g.route, startTimeNanos, g.genTraceId(), pcommon.NewSpanIDEmpty())

	return &traces
}

func (g *TraceGenerator) createSpanForServiceRouteCall(traces *ptrace.Traces, serviceName string, routeName string, startTimeNanos int64, traceId pcommon.TraceID, parentSpanId pcommon.SpanID) *ptrace.Span {
	serviceTier := g.topology.GetServiceTier(serviceName)
	route := serviceTier.GetRoute(routeName)

	if !route.ShouldGenerate() {
		return nil
	}

	rspanSlice := traces.ResourceSpans()
	rspan := rspanSlice.AppendEmpty()

	resource := rspan.Resource()

	resource.Attributes().PutStr(string(semconv.ServiceNameKey), serviceTier.ServiceName)

	resourceAttributeSet := serviceTier.GetResourceAttributeSet(traceId)
	attrs := resource.Attributes()
	resourceAttributeSet.GetAttributes().InsertTags(&attrs)

	rspan.ScopeSpans()
	ils := rspan.ScopeSpans().AppendEmpty()
	spans := ils.Spans()

	span := spans.AppendEmpty()
	newSpanId := g.genSpanId()
	span.SetName(routeName)
	span.SetTraceID(traceId)
	span.SetParentSpanID(parentSpanId)
	span.SetSpanID(newSpanId)
	span.SetKind(ptrace.SpanKindServer)
	span.Attributes().PutStr("load_generator.seq_num", fmt.Sprintf("%v", g.sequenceNumber))

	ts := serviceTier.GetTagSet(routeName, traceId) // ts is single TagSet consisting of tags from the service AND route
	attr := span.Attributes()
	ts.Tags.InsertTags(&attr) // add service and route tags to span attributes

	for _, tg := range ts.TagGenerators {
		tg.Random = g.random
		for k, v := range tg.GenerateTags() {
			span.Attributes().PutStr(k, v) // add generated tags to span attributes
		}
	}

	// TODO: this is still a bit weird - we're calling each downstream route
	// after a sample of the current route's latency, which doesn't really
	// make sense - but maybe it's realistic enough?
	endTime := startTimeNanos + route.SampleLatency(traceId)
	for _, c := range route.DownstreamCalls {
		var childStartTimeNanos = startTimeNanos + route.SampleLatency(traceId)

		childSpan := g.createSpanForServiceRouteCall(traces, c.Service, c.Route, childStartTimeNanos, traceId, newSpanId)
		val, ok := childSpan.Attributes().Get("error")
		if ok {
			errorAttr := span.Attributes().PutEmpty("error")
			val.CopyTo(errorAttr)
		}
		endTime = Max(endTime, int64(childSpan.EndTimestamp()))
	}

	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, startTimeNanos)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, endTime)))
	g.sequenceNumber += 1
	return &span
}

func Max(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}
