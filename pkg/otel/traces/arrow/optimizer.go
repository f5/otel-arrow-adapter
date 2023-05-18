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

// A trace optimizer used to regroup spans by resource and scope.

import (
	"bytes"
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"golang.org/x/exp/maps"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/otlp"
	"github.com/f5/otel-arrow-adapter/pkg/otel/pdata"
)

type (
	TracesOptimizer struct {
		sorter SpanSorter
	}

	TracesOptimized struct {
		Spans []*FlattenedSpan
	}

	ResourceSpanGroup struct {
		Resource          *pcommon.Resource
		ResourceSchemaUrl string
		ScopeSpansIdx     map[string]int // scope span id -> scope span group
		ScopeSpans        []*ScopeSpanGroup
	}

	ScopeSpanGroup struct {
		Scope          *pcommon.InstrumentationScope
		ScopeSchemaUrl string

		SharedData *SharedData
		Spans      []*ptrace.Span
	}

	FlattenedSpan struct {
		// Resource span section.
		ResourceSpanID    string
		Resource          *pcommon.Resource
		ResourceSchemaUrl string

		// Scope span section.
		ScopeSpanID    string
		Scope          *pcommon.InstrumentationScope
		ScopeSchemaUrl string

		// Span section.
		Span *ptrace.Span
	}

	SpanSorter interface {
		Sort(spans []*FlattenedSpan)
	}

	SpansByNothing                                            struct{}
	SpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName struct{}
	SpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId struct{}
	SpansByResourceSpanIdScopeSpanIdNameStartTimestamp        struct{}
	SpansByResourceSpanIdScopeSpanIdNameTraceId               struct{}
	SpansByResourceSpanIdScopeSpanIdTraceIdName               struct{}
	SpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp struct{}

	// SharedData contains all the shared attributes between spans, events, and links.
	SharedData struct {
		sharedAttributes      *common.SharedAttributes
		sharedEventAttributes *common.SharedAttributes
		sharedLinkAttributes  *common.SharedAttributes
	}
)

func NewTracesOptimizer(sorter SpanSorter) *TracesOptimizer {
	return &TracesOptimizer{
		sorter: sorter,
	}
}

func (t *TracesOptimizer) Optimize(traces ptrace.Traces) *TracesOptimized {
	tracesOptimized := &TracesOptimized{
		Spans: make([]*FlattenedSpan, 0),
	}

	resSpans := traces.ResourceSpans()
	for i := 0; i < resSpans.Len(); i++ {
		resSpan := resSpans.At(i)
		resource := resSpan.Resource()
		resourceSchemaUrl := resSpan.SchemaUrl()
		resSpanId := otlp.ResourceID(resource, resourceSchemaUrl)

		scopeSpans := resSpan.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			scopeSpan := scopeSpans.At(j)
			scope := scopeSpan.Scope()
			scopeSchemaUrl := scopeSpan.SchemaUrl()
			scopeSpanId := otlp.ScopeID(scope, scopeSchemaUrl)

			spans := scopeSpan.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				tracesOptimized.Spans = append(tracesOptimized.Spans, &FlattenedSpan{
					ResourceSpanID:    resSpanId,
					Resource:          &resource,
					ResourceSchemaUrl: resourceSchemaUrl,
					ScopeSpanID:       scopeSpanId,
					Scope:             &scope,
					ScopeSchemaUrl:    scopeSchemaUrl,
					Span:              &span,
				})
			}
		}
	}

	t.sorter.Sort(tracesOptimized.Spans)

	return tracesOptimized
}

func (r *ResourceSpanGroup) Sort() {
	for _, scopeSpanGroup := range r.ScopeSpans {
		sort.Slice(scopeSpanGroup.Spans, func(i, j int) bool {
			spanI := scopeSpanGroup.Spans[i]
			spanJ := scopeSpanGroup.Spans[j]

			var traceI [16]byte
			var traceJ [16]byte

			traceI = spanI.TraceID()
			traceJ = spanJ.TraceID()
			cmp := bytes.Compare(traceI[:], traceJ[:])
			if cmp == 0 {
				return spanI.StartTimestamp() < spanJ.StartTimestamp()
			} else {
				return cmp == -1
			}
		})
	}
}

func collectAllSharedAttributes(spans []*ptrace.Span) *SharedData {
	sharedAttrs := make(map[string]pcommon.Value)
	firstSpan := true

	sharedEventAttrs := make(map[string]pcommon.Value)
	firstEvent := true

	sharedLinkAttrs := make(map[string]pcommon.Value)
	firstLink := true

	for i := 0; i < len(spans); i++ {
		span := spans[i]
		attrs := span.Attributes()

		firstSpan = collectSharedAttributes(&attrs, firstSpan, sharedAttrs)

		// Collect shared event attributes
		eventSlice := span.Events()
		if eventSlice.Len() > 1 {
			for j := 0; j < eventSlice.Len(); j++ {
				event := eventSlice.At(j)
				evtAttrs := event.Attributes()

				firstEvent = collectSharedAttributes(&evtAttrs, firstEvent, sharedEventAttrs)
				if len(sharedEventAttrs) == 0 {
					break
				}
			}
		}

		// Collect shared link attributes
		linkSlice := span.Links()
		if linkSlice.Len() > 1 {
			for j := 0; j < linkSlice.Len(); j++ {
				link := linkSlice.At(j)
				linkAttrs := link.Attributes()

				firstLink = collectSharedAttributes(&linkAttrs, firstLink, sharedLinkAttrs)
				if len(sharedLinkAttrs) == 0 {
					break
				}
			}
		}

		if len(sharedAttrs) == 0 && len(sharedEventAttrs) == 0 && len(sharedLinkAttrs) == 0 {
			break
		}
	}

	if len(spans) == 1 {
		sharedAttrs = make(map[string]pcommon.Value)
	}

	return &SharedData{
		sharedAttributes: &common.SharedAttributes{
			Attributes: sharedAttrs,
		},
		sharedEventAttributes: &common.SharedAttributes{
			Attributes: sharedEventAttrs,
		},
		sharedLinkAttributes: &common.SharedAttributes{
			Attributes: sharedLinkAttrs,
		},
	}
}

func collectSharedAttributes(attrs *pcommon.Map, first bool, sharedAttrs map[string]pcommon.Value) bool {
	if first {
		attrs.Range(func(k string, v pcommon.Value) bool {
			sharedAttrs[k] = v
			return true
		})
		return false
	} else {
		if len(sharedAttrs) > 0 {
			if attrs.Len() == 0 {
				maps.Clear(sharedAttrs)
				return first
			}
			for k, v := range sharedAttrs {
				if otherV, ok := attrs.Get(k); ok {
					if !pdata.ValuesEqual(v, otherV) {
						delete(sharedAttrs, k)
					}
				} else {
					delete(sharedAttrs, k)
				}
			}
		}
	}
	return first
}

// No sorting
// ==========

func UnsortedSpans() *SpansByNothing {
	return &SpansByNothing{}
}

func (s *SpansByNothing) Sort(spans []*FlattenedSpan) {
}

// Sorts spans by resource span id, scope span id, start timestamp, trace id, name
// ===============================================================================

func SortSpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName() *SpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName {
	return &SpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName{}
}

func (s *SpansByResourceSpanIdScopeSpanIdStartTimestampTraceIdName) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				if spanI.Span.StartTimestamp() == spanJ.Span.StartTimestamp() {
					var traceI [16]byte
					var traceJ [16]byte

					traceI = spanI.Span.TraceID()
					traceJ = spanJ.Span.TraceID()
					cmp := bytes.Compare(traceI[:], traceJ[:])

					if cmp == 0 {
						return spanI.Span.Name() < spanJ.Span.Name()
					} else {
						return cmp < 0
					}
				} else {
					return spanI.Span.StartTimestamp() < spanJ.Span.StartTimestamp()
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

func SortSpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId() *SpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId {
	return &SpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId{}
}

func (s *SpansByResourceSpanIdScopeSpanIdStartTimestampNameTraceId) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				if spanI.Span.StartTimestamp() == spanJ.Span.StartTimestamp() {
					if spanI.Span.Name() == spanJ.Span.Name() {
						var traceI [16]byte
						var traceJ [16]byte

						traceI = spanI.Span.TraceID()
						traceJ = spanJ.Span.TraceID()
						cmp := bytes.Compare(traceI[:], traceJ[:])
						return cmp < 0
					} else {
						return spanI.Span.Name() < spanJ.Span.Name()
					}
				} else {
					return spanI.Span.StartTimestamp() < spanJ.Span.StartTimestamp()
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

func SortSpansByResourceSpanIdScopeSpanIdNameStartTimestamp() *SpansByResourceSpanIdScopeSpanIdNameStartTimestamp {
	return &SpansByResourceSpanIdScopeSpanIdNameStartTimestamp{}
}

func (s *SpansByResourceSpanIdScopeSpanIdNameStartTimestamp) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				if spanI.Span.Name() == spanJ.Span.Name() {
					return spanI.Span.StartTimestamp() < spanJ.Span.StartTimestamp()
				} else {
					return spanI.Span.Name() < spanJ.Span.Name()
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

func SortSpansByResourceSpanIdScopeSpanIdNameTraceId() *SpansByResourceSpanIdScopeSpanIdNameTraceId {
	return &SpansByResourceSpanIdScopeSpanIdNameTraceId{}
}

func (s *SpansByResourceSpanIdScopeSpanIdNameTraceId) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				if spanI.Span.Name() == spanJ.Span.Name() {
					var traceI [16]byte
					var traceJ [16]byte

					traceI = spanI.Span.TraceID()
					traceJ = spanJ.Span.TraceID()
					cmp := bytes.Compare(traceI[:], traceJ[:])
					return cmp < 0
				} else {
					return spanI.Span.Name() < spanJ.Span.Name()
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

func SortSpansByResourceSpanIdScopeSpanIdTraceIdName() *SpansByResourceSpanIdScopeSpanIdTraceIdName {
	return &SpansByResourceSpanIdScopeSpanIdTraceIdName{}
}

func (s *SpansByResourceSpanIdScopeSpanIdTraceIdName) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				var traceI [16]byte
				var traceJ [16]byte

				traceI = spanI.Span.TraceID()
				traceJ = spanJ.Span.TraceID()
				cmp := bytes.Compare(traceI[:], traceJ[:])

				if cmp == 0 {
					return spanI.Span.Name() < spanJ.Span.Name()
				} else {
					return cmp < 0
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

func SortSpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp() *SpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp {
	return &SpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp{}
}

func (s *SpansByResourceSpanIdScopeSpanIdNameTraceIdStartTimestamp) Sort(spans []*FlattenedSpan) {
	sort.Slice(spans, func(i, j int) bool {
		spanI := spans[i]
		spanJ := spans[j]

		if spanI.ResourceSpanID == spanJ.ResourceSpanID {
			if spanI.ScopeSpanID == spanJ.ScopeSpanID {
				if spanI.Span.Name() == spanJ.Span.Name() {
					var traceI [16]byte
					var traceJ [16]byte

					traceI = spanI.Span.TraceID()
					traceJ = spanJ.Span.TraceID()
					cmp := bytes.Compare(traceI[:], traceJ[:])
					if cmp == 0 {
						return spanI.Span.StartTimestamp() < spanJ.Span.StartTimestamp()
					} else {
						return cmp < 0
					}
				} else {
					return spanI.Span.Name() < spanJ.Span.Name()
				}
			} else {
				return spanI.ScopeSpanID < spanJ.ScopeSpanID
			}
		} else {
			return spanI.ResourceSpanID < spanJ.ResourceSpanID
		}
	})
}

// ToDo TMP
// TraceId, StartTimestamp       -> 1.45 and 1.37
// Name, StartTimestamp          -> 1.52 and 1.50
// Name, TraceId, StartTimestamp -> 1.51 and 1.48
// Name, ResSpanID               -> 1.38 and 1.38
// Name, ResSpanID, ScopeSpanID  -> 1.38 and 1.38

// TraceID					     		-> 1.36 and 1.29
// Name                          		-> 1.38 and 1.38
// StartTimestamp                		-> 1.46 and 1.39
// ResSpanID                     		-> 1.46 and 1.42
// ScopeSpanID                   		-> 1.48 and 1.44

// ScopeSpanID, StartTimestamp   		-> 1.47 and 1.40
// ScopeSpanID, Name			 		-> 1.39 and 1.38
// ScopeSpanID, TraceID          		-> 1.37 and 1.30

// ResSpanID, ScopeSpanID 	     			-> 1.46 and 1.42
// ResSpanID, ScopeSpanID, TraceID 			-> 1.47 and 1.43
// ResSpanID, ScopeSpanID, Name 			-> 1.38 and 1.38
// ResSpanID, ScopeSpanID, StartTimestamp   -> 1.56 and 1.52

// ResSpanID, ScopeSpanID, StartTimestamp, Name		-> 1.56 and 1.52
// ResSpanID, ScopeSpanID, StartTimestamp, TraceId  -> 1.56 and 1.52
// ResSpanID, ScopeSpanID, TraceID, StartTimestamp  -> 1.53 and 1.48
// ResSpanID, ScopeSpanID, Name, StartTimestamp  	-> 1.54 and 1.52

// ResSpanID, ScopeSpanID, StartTimestamp, Name, TraceID	-> 1.56 and 1.52
// ResSpanID, ScopeSpanID, StartTimestamp, TraceId, Name  	-> 1.56 and 1.52
// ResSpanID, ScopeSpanID, StartTimestamp, TraceId, Name  	-> 1.59 and 1.52  (only one group of attrs per resource and scope span)
