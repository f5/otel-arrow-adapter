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

	cfg "github.com/f5/otel-arrow-adapter/pkg/config"
	carrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/stats"
	"github.com/f5/otel-arrow-adapter/pkg/record_message"
)

// Infrastructure to manage metrics related records.

type (
	// RelatedData is a collection of related/dependent data to metrics entities.
	RelatedData struct {
		nextMetricScopeID uint64

		relatedRecordsManager *carrow.RelatedRecordsManager

		attrsBuilders       *AttrsBuilders
		metricsBuilder      *MetricBuilder
		sumNDPBuilder       *NumberDataPointBuilder
		gaugeNDPBuilder     *NumberDataPointBuilder
		histogramDPBuilder  *HistogramDataPointBuilder
		ehistogramDPBuilder *EHistogramDataPointBuilder
	}

	// AttrsBuilders groups together AttrsBuilder instances used to build related
	// data attributes (i.e. resource attributes, scope attributes, metrics
	// attributes.
	AttrsBuilders struct {
		resource *carrow.Attrs16Builder
		scope    *carrow.Attrs16Builder

		// metrics attributes
		sum        *carrow.Attrs32Builder
		gauge      *carrow.Attrs32Builder
		histogram  *carrow.Attrs32Builder
		ehistogram *carrow.Attrs32Builder
	}
)

func NewRelatedData(cfg *cfg.Config, stats *stats.ProducerStats) (*RelatedData, error) {
	rrManager := carrow.NewRelatedRecordsManager(cfg, stats)

	resourceAttrsBuilder := rrManager.Declare(carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16Builder(b, carrow.PayloadTypes.ResourceAttrs)
	})

	scopeAttrsBuilder := rrManager.Declare(carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16Builder(b, carrow.PayloadTypes.ScopeAttrs)
	})

	metricBuilder := rrManager.Declare(MetricSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewMetricBuilder(b)
	})

	sumNDPBuilder := rrManager.Declare(NumberDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		sumBuilder := NewNumberDataPointBuilder(b, carrow.PayloadTypes.Sum)
		metricBuilder.(*MetricBuilder).SetSumAccumulator(sumBuilder.Accumulator())
		return sumBuilder
	})

	sumAttrsBuilder := rrManager.Declare(carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		sab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.SumAttrs)
		sumNDPBuilder.(*NumberDataPointBuilder).SetAttributesAccumulator(sab.Accumulator())
		return sab
	})

	gaugeNDPBuilder := rrManager.Declare(NumberDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		gaugeBuilder := NewNumberDataPointBuilder(b, carrow.PayloadTypes.Gauge)
		metricBuilder.(*MetricBuilder).SetGaugeAccumulator(gaugeBuilder.Accumulator())
		return gaugeBuilder
	})

	gaugeAttrsBuilder := rrManager.Declare(carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		gab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.GaugeAttrs)
		gaugeNDPBuilder.(*NumberDataPointBuilder).SetAttributesAccumulator(gab.Accumulator())
		return gab
	})

	histogramDPBuilder := rrManager.Declare(HistogramDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		histoBuilder := NewHistogramDataPointBuilder(b)
		metricBuilder.(*MetricBuilder).SetHistogramAccumulator(histoBuilder.Accumulator())
		return histoBuilder
	})

	histogramAttrsBuilder := rrManager.Declare(carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		hab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.HistogramAttrs)
		histogramDPBuilder.(*HistogramDataPointBuilder).SetAttributesAccumulator(hab.Accumulator())
		return hab
	})

	ehistogramDPBuilder := rrManager.Declare(EHistogramDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		ehistoBuilder := NewEHistogramDataPointBuilder(b)
		metricBuilder.(*MetricBuilder).SetEHistogramAccumulator(ehistoBuilder.Accumulator())
		return ehistoBuilder
	})

	ehistogramAttrsBuilder := rrManager.Declare(carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		hab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.ExpHistogramAttrs)
		ehistogramDPBuilder.(*EHistogramDataPointBuilder).SetAttributesAccumulator(hab.Accumulator())
		return hab
	})

	return &RelatedData{
		relatedRecordsManager: rrManager,
		attrsBuilders: &AttrsBuilders{
			resource:   resourceAttrsBuilder.(*carrow.Attrs16Builder),
			scope:      scopeAttrsBuilder.(*carrow.Attrs16Builder),
			sum:        sumAttrsBuilder.(*carrow.Attrs32Builder),
			gauge:      gaugeAttrsBuilder.(*carrow.Attrs32Builder),
			histogram:  histogramAttrsBuilder.(*carrow.Attrs32Builder),
			ehistogram: ehistogramAttrsBuilder.(*carrow.Attrs32Builder),
		},
		metricsBuilder:      metricBuilder.(*MetricBuilder),
		sumNDPBuilder:       sumNDPBuilder.(*NumberDataPointBuilder),
		gaugeNDPBuilder:     gaugeNDPBuilder.(*NumberDataPointBuilder),
		histogramDPBuilder:  histogramDPBuilder.(*HistogramDataPointBuilder),
		ehistogramDPBuilder: ehistogramDPBuilder.(*EHistogramDataPointBuilder),
	}, nil
}

func (r *RelatedData) Release() {
	r.relatedRecordsManager.Release()
}

func (r *RelatedData) AttrsBuilders() *AttrsBuilders {
	return r.attrsBuilders
}

func (r *RelatedData) MetricsBuilder() *MetricBuilder {
	return r.metricsBuilder
}

func (r *RelatedData) SumNDPBuilder() *NumberDataPointBuilder {
	return r.sumNDPBuilder
}

func (r *RelatedData) GaugeNDPBuilder() *NumberDataPointBuilder {
	return r.gaugeNDPBuilder
}

func (r *RelatedData) Reset() {
	r.nextMetricScopeID = 0
	r.relatedRecordsManager.Reset()
}

func (r *RelatedData) NextMetricScopeID() uint16 {
	c := r.nextMetricScopeID

	if c == math.MaxUint16 {
		panic("maximum number of scope metrics reached per batch, please reduce the batch size to a maximum of 65535 metrics")
	}

	r.nextMetricScopeID++
	return uint16(c)
}

func (r *RelatedData) BuildRecordMessages() ([]*record_message.RecordMessage, error) {
	return r.relatedRecordsManager.BuildRecordMessages()
}

func (ab *AttrsBuilders) Release() {
	ab.resource.Release()
	ab.scope.Release()
	println("Reintegrate sum and gauge Release!!!")
	//ab.sum.Release()
	//ab.gauge.Release()
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
