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
	resourceRB := builder.NewRecordBuilderExt(cfg.Pool, AttrsSchema, config.NewDictionary(cfg.LimitIndexSize), stats)
	scopeRB := builder.NewRecordBuilderExt(cfg.Pool, AttrsSchema, config.NewDictionary(cfg.LimitIndexSize), stats)
	spanRB := builder.NewRecordBuilderExt(cfg.Pool, AttrsSchema, config.NewDictionary(cfg.LimitIndexSize), stats)
	eventRB := builder.NewRecordBuilderExt(cfg.Pool, AttrsSchema, config.NewDictionary(cfg.LimitIndexSize), stats)
	linkRB := builder.NewRecordBuilderExt(cfg.Pool, AttrsSchema, config.NewDictionary(cfg.LimitIndexSize), stats)

	resourceBuilder, err := NewAttrsBuilder(resourceRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	scopeBuilder, err := NewAttrsBuilder(scopeRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	spanBuilder, err := NewAttrsBuilder(spanRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	eventBuilder, err := NewAttrsBuilder(eventRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	linkBuilder, err := NewAttrsBuilder(linkRB)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &RelatedData{
		attrsBuilders: &AttrsBuilders{
			resource: resourceBuilder,
			scope:    scopeBuilder,
			span:     spanBuilder,
			event:    eventBuilder,
			link:     linkBuilder,
		},
		attrsRecordBuilders: &AttrsRecordBuilders{
			resource: resourceRB,
			scope:    scopeRB,
			span:     spanRB,
			event:    eventRB,
			link:     linkRB,
		},
	}, nil
}

func (r *RelatedData) AttrsBuilders() *AttrsBuilders {
	return r.attrsBuilders
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
	ab.resource.Collector().Reset()
	ab.scope.Collector().Reset()
	ab.span.Collector().Reset()
	ab.event.Collector().Reset()
	ab.link.Collector().Reset()
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
