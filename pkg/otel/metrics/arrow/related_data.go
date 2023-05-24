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

// The definition of all the related data for metrics,

import (
	"math"

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
		sumDPBuilder        *DataPointBuilder
		gaugeDPBuilder      *DataPointBuilder
		summaryDPBuilder    *SummaryDataPointBuilder
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
		summary    *carrow.Attrs32Builder
		gauge      *carrow.Attrs32Builder
		histogram  *carrow.Attrs32Builder
		eHistogram *carrow.Attrs32Builder
	}
)

func NewRelatedData(cfg *Config, stats *stats.ProducerStats) (*RelatedData, error) {
	rrManager := carrow.NewRelatedRecordsManager(cfg.Global, stats)

	resourceAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.ResourceAttrs, carrow.PayloadTypes.Metrics, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16BuilderWithEncoding(b, carrow.PayloadTypes.ResourceAttrs, cfg.Attrs.Resource)
	})

	scopeAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.ScopeAttrs, carrow.PayloadTypes.Metrics, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16BuilderWithEncoding(b, carrow.PayloadTypes.ScopeAttrs, cfg.Attrs.Scope)
	})

	sumDPBuilder := rrManager.Declare(carrow.PayloadTypes.Sum, carrow.PayloadTypes.Metrics, DataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewDataPointBuilder(b, carrow.PayloadTypes.Sum)
	})

	sumAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.SumAttrs, carrow.PayloadTypes.Sum, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		sab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.SumAttrs, cfg.Attrs.Sum)
		sumDPBuilder.(*DataPointBuilder).SetAttributesAccumulator(sab.Accumulator())
		return sab
	})

	gaugeDPBuilder := rrManager.Declare(carrow.PayloadTypes.Gauge, carrow.PayloadTypes.Metrics, DataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewDataPointBuilder(b, carrow.PayloadTypes.Gauge)
	})

	gaugeAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.GaugeAttrs, carrow.PayloadTypes.Gauge, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		gab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.GaugeAttrs, cfg.Attrs.Gauge)
		gaugeDPBuilder.(*DataPointBuilder).SetAttributesAccumulator(gab.Accumulator())
		return gab
	})

	summaryDPBuilder := rrManager.Declare(carrow.PayloadTypes.Summary, carrow.PayloadTypes.Metrics, SummaryDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewSummaryDataPointBuilder(b)
	})

	summaryAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.SummaryAttrs, carrow.PayloadTypes.Summary, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		sab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.SummaryAttrs, cfg.Attrs.Summary)
		summaryDPBuilder.(*SummaryDataPointBuilder).SetAttributesAccumulator(sab.Accumulator())
		return sab
	})

	histogramDPBuilder := rrManager.Declare(carrow.PayloadTypes.Histogram, carrow.PayloadTypes.Metrics, HistogramDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewHistogramDataPointBuilder(b)
	})

	histogramAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.HistogramAttrs, carrow.PayloadTypes.Histogram, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		hab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.HistogramAttrs, cfg.Attrs.Histogram)
		histogramDPBuilder.(*HistogramDataPointBuilder).SetAttributesAccumulator(hab.Accumulator())
		return hab
	})

	ehistogramDPBuilder := rrManager.Declare(carrow.PayloadTypes.ExpHistogram, carrow.PayloadTypes.Metrics, EHistogramDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewEHistogramDataPointBuilder(b)
	})

	ehistogramAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.ExpHistogramAttrs, carrow.PayloadTypes.ExpHistogram, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		hab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.ExpHistogramAttrs, cfg.Attrs.ExpHistogram)
		ehistogramDPBuilder.(*EHistogramDataPointBuilder).SetAttributesAccumulator(hab.Accumulator())
		return hab
	})

	return &RelatedData{
		relatedRecordsManager: rrManager,
		attrsBuilders: &AttrsBuilders{
			resource:   resourceAttrsBuilder.(*carrow.Attrs16Builder),
			scope:      scopeAttrsBuilder.(*carrow.Attrs16Builder),
			sum:        sumAttrsBuilder.(*carrow.Attrs32Builder),
			summary:    summaryAttrsBuilder.(*carrow.Attrs32Builder),
			gauge:      gaugeAttrsBuilder.(*carrow.Attrs32Builder),
			histogram:  histogramAttrsBuilder.(*carrow.Attrs32Builder),
			eHistogram: ehistogramAttrsBuilder.(*carrow.Attrs32Builder),
		},
		sumDPBuilder:        sumDPBuilder.(*DataPointBuilder),
		gaugeDPBuilder:      gaugeDPBuilder.(*DataPointBuilder),
		summaryDPBuilder:    summaryDPBuilder.(*SummaryDataPointBuilder),
		histogramDPBuilder:  histogramDPBuilder.(*HistogramDataPointBuilder),
		ehistogramDPBuilder: ehistogramDPBuilder.(*EHistogramDataPointBuilder),
	}, nil
}

func (r *RelatedData) Schemas() []carrow.SchemaWithPayload {
	return r.relatedRecordsManager.Schemas()
}

func (r *RelatedData) Release() {
	r.relatedRecordsManager.Release()
}

func (r *RelatedData) RecordBuilderExt(payloadType *carrow.PayloadType) *builder.RecordBuilderExt {
	return r.relatedRecordsManager.RecordBuilderExt(payloadType)
}

func (r *RelatedData) AttrsBuilders() *AttrsBuilders {
	return r.attrsBuilders
}

func (r *RelatedData) SumDPBuilder() *DataPointBuilder {
	return r.sumDPBuilder
}

func (r *RelatedData) GaugeDPBuilder() *DataPointBuilder {
	return r.gaugeDPBuilder
}

func (r *RelatedData) SummaryDPBuilder() *SummaryDataPointBuilder {
	return r.summaryDPBuilder
}

func (r *RelatedData) HistogramDPBuilder() *HistogramDataPointBuilder {
	return r.histogramDPBuilder
}

func (r *RelatedData) EHistogramDPBuilder() *EHistogramDataPointBuilder {
	return r.ehistogramDPBuilder
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

	ab.sum.Release()
	ab.gauge.Release()
	ab.summary.Release()
	ab.histogram.Release()
	ab.eHistogram.Release()
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
	ab.sum.Reset()
	ab.gauge.Reset()
	ab.summary.Reset()
	ab.histogram.Reset()
	ab.eHistogram.Reset()
}
