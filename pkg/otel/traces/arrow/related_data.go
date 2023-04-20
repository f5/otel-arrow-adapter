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
	"errors"

	"github.com/apache/arrow/go/v12/arrow"

	colarspb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	config2 "github.com/f5/otel-arrow-adapter/pkg/config"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	config "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/config"
	"github.com/f5/otel-arrow-adapter/pkg/otel/stats"
	"github.com/f5/otel-arrow-adapter/pkg/record_message"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// RelatedData is a collection of related data constructs used by traces.
type RelatedData struct {
	attrsBuilders       *AttrsBuilders
	attrsRecordBuilders *AttrsRecordBuilders

	eventBuilder       *EventBuilder
	eventRecordBuilder *builder.RecordBuilderExt
}

// AttrsBuilders groups together AttrsBuilder instances used to build related
// data attributes (i.e. resource attributes, scope attributes, span attributes,
// event attributes, and link attributes).
type AttrsBuilders struct {
	resource *AttrsBuilder
	scope    *AttrsBuilder
	span     *AttrsBuilder
	event    *AttrsBuilder
	link     *AttrsBuilder
}

// AttrsRecordBuilders is a collection of RecordBuilderExt instances used
// to build related data records (i.e. resource attributes, scope attributes,
// span attributes, event attributes, and link attributes).
type AttrsRecordBuilders struct {
	resource *builder.RecordBuilderExt
	scope    *builder.RecordBuilderExt
	span     *builder.RecordBuilderExt
	event    *builder.RecordBuilderExt
	link     *builder.RecordBuilderExt
}

func NewRelatedData(cfg *config2.Config, stats *stats.ProducerStats) (*RelatedData, error) {
	attrsResourceRB := builder.NewRecordBuilderExt(cfg.Pool, AttrsSchema, config.NewDictionary(cfg.LimitIndexSize), stats)
	attrsScopeRB := builder.NewRecordBuilderExt(cfg.Pool, AttrsSchema, config.NewDictionary(cfg.LimitIndexSize), stats)
	attrsSpanRB := builder.NewRecordBuilderExt(cfg.Pool, AttrsSchema, config.NewDictionary(cfg.LimitIndexSize), stats)
	attrsEventRB := builder.NewRecordBuilderExt(cfg.Pool, AttrsSchema, config.NewDictionary(cfg.LimitIndexSize), stats)
	attrsLinkRB := builder.NewRecordBuilderExt(cfg.Pool, AttrsSchema, config.NewDictionary(cfg.LimitIndexSize), stats)

	attrsResourceBuilder, err := NewAttrsBuilder(attrsResourceRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	attrsScopeBuilder, err := NewAttrsBuilder(attrsScopeRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	attrsSpanBuilder, err := NewAttrsBuilder(attrsSpanRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	attrsEventBuilder, err := NewAttrsBuilder(attrsEventRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	attrsLinkBuilder, err := NewAttrsBuilder(attrsLinkRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	eventRB := builder.NewRecordBuilderExt(cfg.Pool, EventSchema, config.NewDictionary(cfg.LimitIndexSize), stats)
	eventBuilder, err := NewEventBuilder(eventRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &RelatedData{
		attrsBuilders: &AttrsBuilders{
			resource: attrsResourceBuilder,
			scope:    attrsScopeBuilder,
			span:     attrsSpanBuilder,
			event:    attrsEventBuilder,
			link:     attrsLinkBuilder,
		},
		attrsRecordBuilders: &AttrsRecordBuilders{
			resource: attrsResourceRB,
			scope:    attrsScopeRB,
			span:     attrsSpanRB,
			event:    attrsEventRB,
			link:     attrsLinkRB,
		},
		eventBuilder:       eventBuilder,
		eventRecordBuilder: eventRB,
	}, nil
}

func (r *RelatedData) AttrsBuilders() *AttrsBuilders {
	return r.attrsBuilders
}

func (r *RelatedData) EventBuilder() *EventBuilder {
	return r.eventBuilder
}

func (r *RelatedData) Reset() {
	r.attrsBuilders.Reset()
	r.eventBuilder.Accumulator().Reset()
}

func (r *RelatedData) BuildRecordMessages() ([]*record_message.RecordMessage, error) {

	attrsResRec, err := r.attrsBuilders.buildRecord(r.attrsBuilders.resource)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	attrsScopeRec, err := r.attrsBuilders.buildRecord(r.attrsBuilders.scope)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	attrsSpanRec, err := r.attrsBuilders.buildRecord(r.attrsBuilders.span)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	eventRec, err := r.eventBuilder.Build(r.attrsBuilders.event.Accumulator())
	if err != nil {
		return nil, werror.Wrap(err)
	}

	attrsEventRec, err := r.attrsBuilders.buildRecord(r.attrsBuilders.event)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	attrsLinkRec, err := r.attrsBuilders.buildRecord(r.attrsBuilders.link)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return []*record_message.RecordMessage{
		// Resource attributes
		record_message.NewRelatedDataMessage(r.attrsBuilders.resource.SchemaID(), attrsResRec, colarspb.OtlpArrowPayloadType_RESOURCE_ATTRS),
		// Scope attributes
		record_message.NewRelatedDataMessage(r.attrsBuilders.scope.SchemaID(), attrsScopeRec, colarspb.OtlpArrowPayloadType_SCOPE_ATTRS),
		// Span attributes
		record_message.NewRelatedDataMessage(r.attrsBuilders.span.SchemaID(), attrsSpanRec, colarspb.OtlpArrowPayloadType_SPAN_ATTRS),

		// Span events
		record_message.NewRelatedDataMessage(r.eventBuilder.SchemaID(), eventRec, colarspb.OtlpArrowPayloadType_SPAN_EVENTS),
		// Span event attributes
		record_message.NewRelatedDataMessage(r.attrsBuilders.event.SchemaID(), attrsEventRec, colarspb.OtlpArrowPayloadType_SPAN_EVENT_ATTRS),

		// Span link attributes
		record_message.NewRelatedDataMessage(r.attrsBuilders.link.SchemaID(), attrsLinkRec, colarspb.OtlpArrowPayloadType_SPAN_LINK_ATTRS),
	}, nil
}

func (ab *AttrsBuilders) Resource() *AttrsBuilder {
	return ab.resource
}

func (ab *AttrsBuilders) Scope() *AttrsBuilder {
	return ab.scope
}

func (ab *AttrsBuilders) Span() *AttrsBuilder {
	return ab.span
}

func (ab *AttrsBuilders) Event() *AttrsBuilder {
	return ab.event
}

func (ab *AttrsBuilders) Link() *AttrsBuilder {
	return ab.link
}

func (ab *AttrsBuilders) Reset() {
	ab.resource.Accumulator().Reset()
	ab.scope.Accumulator().Reset()
	ab.span.Accumulator().Reset()
	ab.event.Accumulator().Reset()
	ab.link.Accumulator().Reset()
}

func (ab *AttrsBuilders) BuildRecordMessages() ([]*record_message.RecordMessage, error) {
	resourceRecord, err := ab.buildRecord(ab.resource)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	scopeRecord, err := ab.buildRecord(ab.scope)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	spanRecord, err := ab.buildRecord(ab.span)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	eventRecord, err := ab.buildRecord(ab.event)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	linkRecord, err := ab.buildRecord(ab.link)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	resourceRD := record_message.NewRelatedDataMessage(ab.resource.SchemaID(), resourceRecord, colarspb.OtlpArrowPayloadType_RESOURCE_ATTRS)
	scopeRD := record_message.NewRelatedDataMessage(ab.scope.SchemaID(), scopeRecord, colarspb.OtlpArrowPayloadType_SCOPE_ATTRS)
	spanRD := record_message.NewRelatedDataMessage(ab.span.SchemaID(), spanRecord, colarspb.OtlpArrowPayloadType_SPAN_ATTRS)
	eventRD := record_message.NewRelatedDataMessage(ab.event.SchemaID(), eventRecord, colarspb.OtlpArrowPayloadType_SPAN_EVENT_ATTRS)
	linkRD := record_message.NewRelatedDataMessage(ab.link.SchemaID(), linkRecord, colarspb.OtlpArrowPayloadType_SPAN_LINK_ATTRS)

	return []*record_message.RecordMessage{
		resourceRD,
		scopeRD,
		spanRD,
		eventRD,
		linkRD,
	}, nil
}

func (ab *AttrsBuilders) buildRecord(attrsBuilder *AttrsBuilder) (arrow.Record, error) {
	schemaNotUpToDateCount := 0

	var record arrow.Record
	var err error

	// Loop until the record is built successfully.
	// Intermediaries steps may be required to update the schema.
	for {
		record, err = attrsBuilder.Build()
		if err != nil {
			if record != nil {
				record.Release()
			}

			switch {
			case errors.Is(err, schema.ErrSchemaNotUpToDate):
				schemaNotUpToDateCount++
				if schemaNotUpToDateCount > 5 {
					panic("Too many consecutive schema updates. This shouldn't happen.")
				}
			default:
				return nil, werror.Wrap(err)
			}
		} else {
			break
		}
	}
	return record, werror.Wrap(err)
}

func (arb *AttrsRecordBuilders) Resource() *builder.RecordBuilderExt {
	return arb.resource
}

func (arb *AttrsRecordBuilders) Scope() *builder.RecordBuilderExt {
	return arb.scope
}

func (arb *AttrsRecordBuilders) Span() *builder.RecordBuilderExt {
	return arb.span
}

func (arb *AttrsRecordBuilders) Event() *builder.RecordBuilderExt {
	return arb.event
}

func (arb *AttrsRecordBuilders) Link() *builder.RecordBuilderExt {
	return arb.link
}
