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

	colarspb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	cfg "github.com/f5/otel-arrow-adapter/pkg/config"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	config "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/config"
	"github.com/f5/otel-arrow-adapter/pkg/otel/stats"
	"github.com/f5/otel-arrow-adapter/pkg/record_message"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

type (
	RelatedRecordBuilder interface {
		IsEmpty() bool
		Build() (arrow.Record, error)
		SchemaID() string
		PayloadType() *PayloadType
		Reset()
		Release()
	}

	RelatedRecordsManager struct {
		cfg   *cfg.Config
		stats *stats.ProducerStats

		builders    []RelatedRecordBuilder
		builderExts []*builder.RecordBuilderExt
	}

	PayloadType struct {
		prefix      string
		payloadType record_message.PayloadType
	}

	payloadTypes struct {
		ResourceAttrs     *PayloadType
		ScopeAttrs        *PayloadType
		Metric            *PayloadType
		Gauge             *PayloadType
		GaugeAttrs        *PayloadType
		Sum               *PayloadType
		SumAttrs          *PayloadType
		Histogram         *PayloadType
		HistogramAttrs    *PayloadType
		ExpHistogram      *PayloadType
		ExpHistogramAttrs *PayloadType
		LogRecordAttrs    *PayloadType
		SpanAttrs         *PayloadType
		Event             *PayloadType
		EventAttrs        *PayloadType
		Link              *PayloadType
		LinkAttrs         *PayloadType
	}
)

var (
	PayloadTypes = payloadTypes{
		ResourceAttrs: &PayloadType{
			prefix:      "resource-attrs",
			payloadType: colarspb.OtlpArrowPayloadType_RESOURCE_ATTRS,
		},
		ScopeAttrs: &PayloadType{
			prefix:      "scope-attrs",
			payloadType: colarspb.OtlpArrowPayloadType_SCOPE_ATTRS,
		},
		Metric: &PayloadType{
			prefix:      "metric",
			payloadType: colarspb.OtlpArrowPayloadType_METRIC,
		},
		Gauge: &PayloadType{
			prefix:      "gauge",
			payloadType: colarspb.OtlpArrowPayloadType_GAUGES,
		},
		GaugeAttrs: &PayloadType{
			prefix:      "gauge-attrs",
			payloadType: colarspb.OtlpArrowPayloadType_GAUGE_ATTRS,
		},
		Sum: &PayloadType{
			prefix:      "sum",
			payloadType: colarspb.OtlpArrowPayloadType_SUMS,
		},
		SumAttrs: &PayloadType{
			prefix:      "sum-attrs",
			payloadType: colarspb.OtlpArrowPayloadType_SUM_ATTRS,
		},
		Histogram: &PayloadType{
			prefix:      "histogram",
			payloadType: colarspb.OtlpArrowPayloadType_HISTOGRAMS,
		},
		HistogramAttrs: &PayloadType{
			prefix:      "histogram-attrs",
			payloadType: colarspb.OtlpArrowPayloadType_HISTOGRAM_ATTRS,
		},
		ExpHistogram: &PayloadType{
			prefix:      "exp-histogram",
			payloadType: colarspb.OtlpArrowPayloadType_EXP_HISTOGRAMS,
		},
		ExpHistogramAttrs: &PayloadType{
			prefix:      "exp-histogram-attrs",
			payloadType: colarspb.OtlpArrowPayloadType_EXP_HISTOGRAM_ATTRS,
		},
		LogRecordAttrs: &PayloadType{
			prefix:      "logs-attrs",
			payloadType: colarspb.OtlpArrowPayloadType_LOG_ATTRS,
		},
		SpanAttrs: &PayloadType{
			prefix:      "span-attrs",
			payloadType: colarspb.OtlpArrowPayloadType_SPAN_ATTRS,
		},
		Event: &PayloadType{
			prefix:      "span-event",
			payloadType: colarspb.OtlpArrowPayloadType_SPAN_EVENTS,
		},
		EventAttrs: &PayloadType{
			prefix:      "span-event-attrs",
			payloadType: colarspb.OtlpArrowPayloadType_SPAN_EVENT_ATTRS,
		},
		Link: &PayloadType{
			prefix:      "span-link",
			payloadType: colarspb.OtlpArrowPayloadType_SPAN_LINKS,
		},
		LinkAttrs: &PayloadType{
			prefix:      "span-link-attrs",
			payloadType: colarspb.OtlpArrowPayloadType_SPAN_LINK_ATTRS,
		},
	}
)

func NewRelatedRecordsManager(cfg *cfg.Config, stats *stats.ProducerStats) *RelatedRecordsManager {
	return &RelatedRecordsManager{
		cfg:         cfg,
		stats:       stats,
		builders:    make([]RelatedRecordBuilder, 0),
		builderExts: make([]*builder.RecordBuilderExt, 0),
	}
}

func (m *RelatedRecordsManager) Declare(schema *arrow.Schema, rrBuilder func(b *builder.RecordBuilderExt) RelatedRecordBuilder) RelatedRecordBuilder {
	builderExt := builder.NewRecordBuilderExt(m.cfg.Pool, schema, config.NewDictionary(m.cfg.LimitIndexSize), m.stats)
	rBuilder := rrBuilder(builderExt)
	m.builders = append(m.builders, rBuilder)
	m.builderExts = append(m.builderExts, builderExt)
	return rBuilder
}

func (m *RelatedRecordsManager) BuildRecordMessages() ([]*record_message.RecordMessage, error) {
	recordMessages := make([]*record_message.RecordMessage, 0, len(m.builders))
	for _, b := range m.builders {
		if b.IsEmpty() {
			continue
		}
		record, err := b.Build()
		if err != nil {
			return nil, werror.WrapWithContext(
				err,
				map[string]interface{}{"schema_prefix": b.PayloadType().SchemaPrefix()},
			)
		}
		schemaID := b.PayloadType().SchemaPrefix() + ":" + b.SchemaID()
		relatedDataMessage := record_message.NewRelatedDataMessage(schemaID, record, b.PayloadType().PayloadType())
		recordMessages = append(recordMessages, relatedDataMessage)
	}
	return recordMessages, nil
}

func (m *RelatedRecordsManager) Reset() {
	for _, b := range m.builders {
		b.Reset()
	}
}

func (m *RelatedRecordsManager) Release() {
	for _, b := range m.builders {
		b.Release()
	}
	for _, b := range m.builderExts {
		b.Release()
	}
}

func (m *RelatedRecordsManager) RecordBuilderExt(payloadType *PayloadType) *builder.RecordBuilderExt {
	for i, b := range m.builders {
		if b.PayloadType() == payloadType {
			return m.builderExts[i]
		}
	}
	return nil
}

func (p *PayloadType) SchemaPrefix() string {
	return p.prefix
}

func (p *PayloadType) PayloadType() record_message.PayloadType {
	return p.payloadType
}
