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
		sumIDPBuilder       *IntDataPointBuilder
		sumDDPBuilder       *DoubleDataPointBuilder
		gaugeIDPBuilder     *IntDataPointBuilder
		gaugeDDPBuilder     *DoubleDataPointBuilder
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
		intSum      *carrow.Attrs32Builder
		doubleSum   *carrow.Attrs32Builder
		summary     *carrow.Attrs32Builder
		intGauge    *carrow.Attrs32Builder
		doubleGauge *carrow.Attrs32Builder
		histogram   *carrow.Attrs32Builder
		ehistogram  *carrow.Attrs32Builder
	}
)

func NewRelatedData(cfg *cfg.Config, stats *stats.ProducerStats) (*RelatedData, error) {
	rrManager := carrow.NewRelatedRecordsManager(cfg, stats)

	resourceAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.ResourceAttrs, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16Builder(b, carrow.PayloadTypes.ResourceAttrs)
	})

	scopeAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.ScopeAttrs, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16Builder(b, carrow.PayloadTypes.ScopeAttrs)
	})

	sumIDPBuilder := rrManager.Declare(carrow.PayloadTypes.IntSum, IntDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewIntDataPointBuilder(b, carrow.PayloadTypes.IntSum)
	})

	intSumAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.IntSumAttrs, carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		sab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.IntSumAttrs)
		sumIDPBuilder.(*IntDataPointBuilder).SetAttributesAccumulator(sab.Accumulator())
		return sab
	})

	sumDDPBuilder := rrManager.Declare(carrow.PayloadTypes.DoubleSum, DoubleDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewDoubleDataPointBuilder(b, carrow.PayloadTypes.DoubleSum)
	})

	doubleSumAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.DoubleSumAttrs, carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		sab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.DoubleSumAttrs)
		sumDDPBuilder.(*DoubleDataPointBuilder).SetAttributesAccumulator(sab.Accumulator())
		return sab
	})

	gaugeIDPBuilder := rrManager.Declare(carrow.PayloadTypes.IntGauge, IntDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewIntDataPointBuilder(b, carrow.PayloadTypes.IntGauge)
	})

	intGaugeAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.IntGaugeAttrs, carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		gab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.IntGaugeAttrs)
		gaugeIDPBuilder.(*IntDataPointBuilder).SetAttributesAccumulator(gab.Accumulator())
		return gab
	})

	gaugeDDPBuilder := rrManager.Declare(carrow.PayloadTypes.DoubleGauge, DoubleDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewDoubleDataPointBuilder(b, carrow.PayloadTypes.DoubleGauge)
	})

	doubleGaugeAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.DoubleGaugeAttrs, carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		gab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.DoubleGaugeAttrs)
		gaugeDDPBuilder.(*DoubleDataPointBuilder).SetAttributesAccumulator(gab.Accumulator())
		return gab
	})

	summaryDPBuilder := rrManager.Declare(carrow.PayloadTypes.Summary, SummaryDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewSummaryDataPointBuilder(b)
	})

	summaryAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.SummaryAttrs, carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		sab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.SummaryAttrs)
		summaryDPBuilder.(*SummaryDataPointBuilder).SetAttributesAccumulator(sab.Accumulator())
		return sab
	})

	histogramDPBuilder := rrManager.Declare(carrow.PayloadTypes.Histogram, HistogramDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewHistogramDataPointBuilder(b)
	})

	histogramAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.HistogramAttrs, carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		hab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.HistogramAttrs)
		histogramDPBuilder.(*HistogramDataPointBuilder).SetAttributesAccumulator(hab.Accumulator())
		return hab
	})

	ehistogramDPBuilder := rrManager.Declare(carrow.PayloadTypes.ExpHistogram, EHistogramDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewEHistogramDataPointBuilder(b)
	})

	ehistogramAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.ExpHistogramAttrs, carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		hab := carrow.NewAttrs32Builder(b, carrow.PayloadTypes.ExpHistogramAttrs)
		ehistogramDPBuilder.(*EHistogramDataPointBuilder).SetAttributesAccumulator(hab.Accumulator())
		return hab
	})

	return &RelatedData{
		relatedRecordsManager: rrManager,
		attrsBuilders: &AttrsBuilders{
			resource:    resourceAttrsBuilder.(*carrow.Attrs16Builder),
			scope:       scopeAttrsBuilder.(*carrow.Attrs16Builder),
			intSum:      intSumAttrsBuilder.(*carrow.Attrs32Builder),
			doubleSum:   doubleSumAttrsBuilder.(*carrow.Attrs32Builder),
			summary:     summaryAttrsBuilder.(*carrow.Attrs32Builder),
			intGauge:    intGaugeAttrsBuilder.(*carrow.Attrs32Builder),
			doubleGauge: doubleGaugeAttrsBuilder.(*carrow.Attrs32Builder),
			histogram:   histogramAttrsBuilder.(*carrow.Attrs32Builder),
			ehistogram:  ehistogramAttrsBuilder.(*carrow.Attrs32Builder),
		},
		sumIDPBuilder:       sumIDPBuilder.(*IntDataPointBuilder),
		sumDDPBuilder:       sumDDPBuilder.(*DoubleDataPointBuilder),
		gaugeIDPBuilder:     gaugeIDPBuilder.(*IntDataPointBuilder),
		gaugeDDPBuilder:     gaugeDDPBuilder.(*DoubleDataPointBuilder),
		summaryDPBuilder:    summaryDPBuilder.(*SummaryDataPointBuilder),
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

func (r *RelatedData) SumIDPBuilder() *IntDataPointBuilder {
	return r.sumIDPBuilder
}

func (r *RelatedData) SumDDPBuilder() *DoubleDataPointBuilder {
	return r.sumDDPBuilder
}

func (r *RelatedData) GaugeIDPBuilder() *IntDataPointBuilder {
	return r.gaugeIDPBuilder
}

func (r *RelatedData) GaugeDDPBuilder() *DoubleDataPointBuilder {
	return r.gaugeDDPBuilder
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
	return ab.intSum
}

func (ab *AttrsBuilders) Gauge() *carrow.Attrs32Builder {
	return ab.intGauge
}

func (ab *AttrsBuilders) Reset() {
	ab.resource.Accumulator().Reset()
	ab.scope.Accumulator().Reset()
	ab.intSum.Accumulator().Reset()
	ab.intGauge.Accumulator().Reset()
}
