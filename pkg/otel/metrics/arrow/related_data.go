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
	"math"

	colarspb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	cfg "github.com/f5/otel-arrow-adapter/pkg/config"
	carrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	config "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/config"
	"github.com/f5/otel-arrow-adapter/pkg/otel/stats"
	"github.com/f5/otel-arrow-adapter/pkg/record_message"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// Infrastructure to manage metrics related records.

type (
	// RelatedData is a collection of related/dependent data to metrics entities.
	RelatedData struct {
		metricsCount uint64

		attrsBuilders       *AttrsBuilders
		attrsRecordBuilders *AttrsRecordBuilders

		nextSumID   uint64
		nextGaugeID uint64
	}

	// AttrsBuilders groups together AttrsBuilder instances used to build related
	// data attributes (i.e. resource attributes, scope attributes, metrics
	// attributes.
	AttrsBuilders struct {
		resource *carrow.Attrs16Builder
		scope    *carrow.Attrs16Builder

		// metrics attributes
		sum   *carrow.Attrs32Builder
		gauge *carrow.Attrs32Builder
	}

	// AttrsRecordBuilders is a collection of RecordBuilderExt instances used
	// to build related data records (i.e. resource attributes, scope attributes,
	// metrics attributes.
	AttrsRecordBuilders struct {
		resource *builder.RecordBuilderExt
		scope    *builder.RecordBuilderExt

		// metrics attributes
		sum   *builder.RecordBuilderExt
		gauge *builder.RecordBuilderExt
	}
)

func NewRelatedData(cfg *cfg.Config, stats *stats.ProducerStats) (*RelatedData, error) {
	resourceAttrsRB := builder.NewRecordBuilderExt(cfg.Pool, carrow.AttrsSchema16, config.NewDictionary(cfg.LimitIndexSize), stats)
	scopeAttrsRB := builder.NewRecordBuilderExt(cfg.Pool, carrow.AttrsSchema16, config.NewDictionary(cfg.LimitIndexSize), stats)
	sumAttrsRB := builder.NewRecordBuilderExt(cfg.Pool, carrow.AttrsSchema32, config.NewDictionary(cfg.LimitIndexSize), stats)
	gaugeAttrsRB := builder.NewRecordBuilderExt(cfg.Pool, carrow.AttrsSchema32, config.NewDictionary(cfg.LimitIndexSize), stats)

	resourceAttrsBuilder := carrow.NewAttrs16Builder(resourceAttrsRB, carrow.PayloadTypes.ResourceAttrs)
	scopeAttrsBuilder := carrow.NewAttrs16Builder(scopeAttrsRB, carrow.PayloadTypes.ScopeAttrs)
	sumAttrsBuilder := carrow.NewAttrs32Builder(sumAttrsRB, carrow.PayloadTypes.SumAttrs)
	gaugeAttrsBuilder := carrow.NewAttrs32Builder(gaugeAttrsRB, carrow.PayloadTypes.GaugeAttrs)

	return &RelatedData{
		attrsBuilders: &AttrsBuilders{
			resource: resourceAttrsBuilder,
			scope:    scopeAttrsBuilder,
			sum:      sumAttrsBuilder,
			gauge:    gaugeAttrsBuilder,
		},
		attrsRecordBuilders: &AttrsRecordBuilders{
			resource: resourceAttrsRB,
			scope:    scopeAttrsRB,
			sum:      sumAttrsRB,
			gauge:    gaugeAttrsRB,
		},
	}, nil
}

func (r *RelatedData) Release() {
	if r.attrsBuilders != nil {
		r.attrsBuilders.Release()
	}
	if r.attrsRecordBuilders != nil {
		r.attrsRecordBuilders.Release()
	}
}

func (r *RelatedData) AttrsBuilders() *AttrsBuilders {
	return r.attrsBuilders
}

func (r *RelatedData) AttrsRecordBuilders() *AttrsRecordBuilders {
	return r.attrsRecordBuilders
}

func (r *RelatedData) Reset() {
	r.metricsCount = 0
	r.attrsBuilders.Reset()
}

func (r *RelatedData) MetricsCount() uint32 {
	return uint32(r.metricsCount)
}

func (r *RelatedData) NextMetricsID() uint32 {
	c := r.metricsCount

	if c == math.MaxUint32 {
		panic("maximum number of metrics reached per batch, please reduce the batch size to a maximum of 4294967295 metrics")
	}

	r.metricsCount++
	return uint32(c)
}

func (r *RelatedData) NextSumID() uint32 {
	c := r.nextSumID

	if c == math.MaxUint32 {
		panic("maximum number of sums reached per batch, please reduce the batch size to a maximum of 4294967295 metrics")
	}

	r.nextSumID++
	return uint32(c)
}

func (r *RelatedData) NextGaugeID() uint32 {
	c := r.nextGaugeID

	if c == math.MaxUint32 {
		panic("maximum number of gauges reached per batch, please reduce the batch size to a maximum of 4294967295 metrics")
	}

	r.nextGaugeID++
	return uint32(c)
}

func (r *RelatedData) BuildRecordMessages() ([]*record_message.RecordMessage, error) {
	recordMessages := make([]*record_message.RecordMessage, 0, 6)

	if !r.attrsBuilders.resource.IsEmpty() {
		attrsResRec, err := r.attrsBuilders.resource.Build()
		if err != nil {
			return nil, werror.Wrap(err)
		}
		schemaID := "resource_attrs:" + r.attrsBuilders.resource.SchemaID()
		recordMessages = append(recordMessages, record_message.NewRelatedDataMessage(schemaID, attrsResRec, colarspb.OtlpArrowPayloadType_RESOURCE_ATTRS))
	}

	if !r.attrsBuilders.scope.IsEmpty() {
		attrsScopeRec, err := r.attrsBuilders.scope.Build()
		if err != nil {
			return nil, werror.Wrap(err)
		}
		schemaID := "scope_attrs:" + r.attrsBuilders.scope.SchemaID()
		recordMessages = append(recordMessages, record_message.NewRelatedDataMessage(schemaID, attrsScopeRec, colarspb.OtlpArrowPayloadType_SCOPE_ATTRS))
	}

	if !r.attrsBuilders.sum.IsEmpty() {
		attrsMetricsRec, err := r.attrsBuilders.sum.Build()
		if err != nil {
			return nil, werror.Wrap(err)
		}
		schemaID := "sum_attrs:" + r.attrsBuilders.sum.SchemaID()
		recordMessages = append(recordMessages, record_message.NewRelatedDataMessage(schemaID, attrsMetricsRec, colarspb.OtlpArrowPayloadType_SUM_ATTRS))
	}

	if !r.attrsBuilders.gauge.IsEmpty() {
		attrsMetricsRec, err := r.attrsBuilders.gauge.Build()
		if err != nil {
			return nil, werror.Wrap(err)
		}
		schemaID := "gauge_attrs:" + r.attrsBuilders.gauge.SchemaID()
		recordMessages = append(recordMessages, record_message.NewRelatedDataMessage(schemaID, attrsMetricsRec, colarspb.OtlpArrowPayloadType_GAUGE_ATTRS))
	}

	return recordMessages, nil
}

func (ab *AttrsBuilders) Release() {
	ab.resource.Release()
	ab.scope.Release()
	ab.sum.Release()
	ab.gauge.Release()
}

func (ab *AttrsBuilders) Resource() *carrow.Attrs16Builder {
	return ab.resource
}

func (ab *AttrsBuilders) Scope() *carrow.Attrs16Builder {
	return ab.scope
}

func (ab *AttrsBuilders) Sum() *carrow.Attrs32Builder {
	return ab.sum
}

func (ab *AttrsBuilders) Gauge() *carrow.Attrs32Builder {
	return ab.gauge
}

func (ab *AttrsBuilders) Reset() {
	ab.resource.Accumulator().Reset()
	ab.scope.Accumulator().Reset()
	ab.sum.Accumulator().Reset()
	ab.gauge.Accumulator().Reset()
}

func (arb *AttrsRecordBuilders) Release() {
	arb.resource.Release()
	arb.scope.Release()
	arb.sum.Release()
	arb.gauge.Release()
}

func (arb *AttrsRecordBuilders) Resource() *builder.RecordBuilderExt {
	return arb.resource
}

func (arb *AttrsRecordBuilders) Scope() *builder.RecordBuilderExt {
	return arb.scope
}

func (arb *AttrsRecordBuilders) SumAttributes() *builder.RecordBuilderExt {
	return arb.sum
}

func (arb *AttrsRecordBuilders) GaugeAttributes() *builder.RecordBuilderExt {
	return arb.gauge
}
