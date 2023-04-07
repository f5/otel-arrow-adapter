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
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/otel/pdata"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// ScopeSpansDT is the Arrow Data Type describing a scope span.
var (
	ScopeSpansDT = arrow.StructOf([]arrow.Field{
		{Name: constants.Scope, Type: acommon.ScopeDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SchemaUrl, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.Spans, Type: arrow.ListOf(SpanDT)},
		{Name: constants.SharedAttributes, Type: acommon.AttributesDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SharedEventAttributes, Type: acommon.AttributesDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SharedLinkAttributes, Type: acommon.AttributesDT, Metadata: schema.Metadata(schema.Optional)},
	}...)
)

// ScopeSpansBuilder is a helper to build a scope spans.
type ScopeSpansBuilder struct {
	released bool

	builder *builder.StructBuilder

	scb  *acommon.ScopeBuilder      // `scope` builder
	schb *builder.StringBuilder     // `schema_url` builder
	ssb  *builder.ListBuilder       // `spans` list builder
	sb   *SpanBuilder               // `span` builder
	sab  *acommon.AttributesBuilder // `shared_attributes` builder
	seab *acommon.AttributesBuilder // `shared_event_attributes` builder
	slab *acommon.AttributesBuilder // `shared_link_attributes` builder
}

// SharedData contains all the shared attributes between spans, events, and links.
type SharedData struct {
	sharedAttributes      *common.SharedAttributes
	sharedEventAttributes *common.SharedAttributes
	sharedLinkAttributes  *common.SharedAttributes
}

func ScopeSpansBuilderFrom(builder *builder.StructBuilder) *ScopeSpansBuilder {
	ssb := builder.ListBuilder(constants.Spans)

	return &ScopeSpansBuilder{
		released: false,
		builder:  builder,
		scb:      acommon.ScopeBuilderFrom(builder.StructBuilder(constants.Scope)),
		schb:     builder.StringBuilder(constants.SchemaUrl),
		ssb:      ssb,
		sb:       SpanBuilderFrom(ssb.StructBuilder()),
		sab:      acommon.AttributesBuilderFrom(builder.MapBuilder(constants.SharedAttributes)),
		seab:     acommon.AttributesBuilderFrom(builder.MapBuilder(constants.SharedEventAttributes)),
		slab:     acommon.AttributesBuilderFrom(builder.MapBuilder(constants.SharedLinkAttributes)),
	}
}

// Build builds the scope spans array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ScopeSpansBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Append appends a new scope spans to the builder.
func (b *ScopeSpansBuilder) Append(spg *ScopeSpanGroup) error {
	if b.released {
		return werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	return b.builder.Append(spg, func() error {
		if err := b.scb.Append(spg.Scope); err != nil {
			return werror.Wrap(err)
		}
		b.schb.AppendNonEmpty(spg.ScopeSchemaUrl)
		sc := len(spg.Spans)

		sharedData := collectAllSharedAttributes(spg.Spans)

		// Append span shared attributes
		if err := appendSharedAttributes(sharedData.sharedAttributes, b.sab); err != nil {
			return werror.Wrap(err)
		}

		// Append event shared attributes
		if err := appendSharedAttributes(sharedData.sharedEventAttributes, b.seab); err != nil {
			return werror.Wrap(err)
		}

		// shared link shared attributes
		if err := appendSharedAttributes(sharedData.sharedLinkAttributes, b.slab); err != nil {
			return werror.Wrap(err)
		}

		return b.ssb.Append(sc, func() error {
			for i := 0; i < sc; i++ {
				if err := b.sb.Append(spg.Spans[i], sharedData); err != nil {
					return werror.Wrap(err)
				}
			}
			return nil
		})
	})
}

func appendSharedAttributes(sharedAttrs *common.SharedAttributes, builder *acommon.AttributesBuilder) error {
	if len(sharedAttrs.Attributes) > 0 {
		attrs := pcommon.NewMap()
		sharedAttrs.CopyTo(attrs)
		if err := builder.Append(attrs); err != nil {
			return werror.Wrap(err)
		}
	} else {
		if err := builder.AppendNull(); err != nil {
			return werror.Wrap(err)
		}
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *ScopeSpansBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}

func collectAllSharedAttributes(spans []*ptrace.Span) *SharedData {
	sharedAttrs := common.SharedAttributes{
		Attributes: make(map[string]pcommon.Value),
	}
	firstSpan := true

	sharedEventAttrs := common.SharedAttributes{
		Attributes: make(map[string]pcommon.Value),
	}
	firstEvent := true

	sharedLinkAttrs := common.SharedAttributes{
		Attributes: make(map[string]pcommon.Value),
	}
	firstLink := true

	sharedData := SharedData{
		sharedAttributes:      &sharedAttrs,
		sharedEventAttributes: &sharedEventAttrs,
		sharedLinkAttributes:  &sharedLinkAttrs,
	}

	for i := 0; i < len(spans); i++ {
		span := spans[i]
		attrs := span.Attributes()

		firstSpan = collectSharedAttributes(&attrs, firstSpan, &sharedAttrs)

		// Collect shared event attributes
		eventSlice := span.Events()
		if eventSlice.Len() > 1 {
			for j := 0; j < eventSlice.Len(); j++ {
				event := eventSlice.At(j)
				evtAttrs := event.Attributes()

				firstEvent = collectSharedAttributes(&evtAttrs, firstEvent, &sharedEventAttrs)
			}
		}

		// Collect shared link attributes
		linkSlice := span.Links()
		if linkSlice.Len() > 1 {
			for j := 0; j < linkSlice.Len(); j++ {
				link := linkSlice.At(j)
				linkAttrs := link.Attributes()

				firstLink = collectSharedAttributes(&linkAttrs, firstLink, &sharedLinkAttrs)
			}
		}

		if len(sharedAttrs.Attributes) == 0 && len(sharedEventAttrs.Attributes) == 0 && len(sharedLinkAttrs.Attributes) == 0 {
			break
		}
	}

	return &sharedData
}

func collectSharedAttributes(attrs *pcommon.Map, first bool, sharedAttrs *common.SharedAttributes) bool {
	if first {
		attrs.Range(func(k string, v pcommon.Value) bool {
			sharedAttrs.Attributes[k] = v
			return true
		})
		return false
	} else {
		if len(sharedAttrs.Attributes) > 0 {
			if attrs.Len() == 0 {
				sharedAttrs.Attributes = make(map[string]pcommon.Value)
			}
			for k, v := range sharedAttrs.Attributes {
				if otherV, ok := attrs.Get(k); ok {
					if !pdata.ValuesEqual(v, otherV) {
						delete(sharedAttrs.Attributes, k)
					}
				} else {
					delete(sharedAttrs.Attributes, k)
				}
			}
		}
	}
	return first
}
