// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrow

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/apache/arrow/go/v12/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

var (
	MetricSchema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint16, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding)},
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint16},
		{Name: constants.Name, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.Description, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.Unit, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.AggregationTemporality, Type: arrow.PrimitiveTypes.Int32, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.IsMonotonic, Type: arrow.FixedWidthTypes.Boolean, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SharedAttributes, Type: acommon.AttributesDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SharedStartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SharedTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

type (
	// MetricBuilder is a helper to build a metric set.
	MetricBuilder struct {
		released bool

		builder *builder.RecordBuilderExt

		ib     *builder.Uint16DeltaBuilder // `id` builder
		pib    *builder.Uint16Builder      // `parent_id` builder
		nb     *builder.StringBuilder      // metric name builder
		db     *builder.StringBuilder      // metric description builder
		ub     *builder.StringBuilder      // metric unit builder
		atb    *builder.Int32Builder       // aggregation temporality builder
		imb    *builder.BooleanBuilder     // is monotonic builder
		sab    *acommon.AttributesBuilder  // shared attributes builder
		sstunb *builder.TimestampBuilder   // shared start time unix nano builder
		stunb  *builder.TimestampBuilder   // shared time unix nano builder

		accumulator            *MetricAccumulator
		intSumAccumulator      *IDPAccumulator
		doubleSumAccumulator   *DDPAccumulator
		summaryAccumulator     *SummaryAccumulator
		intGaugeAccumulator    *IDPAccumulator
		doubleGaugeAccumulator *DDPAccumulator
		histogramAccumulator   *HDPAccumulator
		ehistogramAccumulator  *EHDPAccumulator
	}

	Metric struct {
		ParentID                uint16
		Name                    string
		Description             string
		Unit                    string
		MetricType              pmetric.MetricType
		Metric                  *pmetric.Metric
		SMData                  *ScopeMetricsSharedData
		MData                   *MetricSharedData
		SharedStartTimeUnixNano *pcommon.Timestamp
		SharedTimeUnixNano      *pcommon.Timestamp
	}

	MetricAccumulator struct {
		groupCount uint16
		metrics    []Metric
	}
)

func NewMetricBuilder(rBuilder *builder.RecordBuilderExt) *MetricBuilder {
	b := &MetricBuilder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewMetricAccumulator(),
	}

	b.init()
	return b
}

func (b *MetricBuilder) init() {
	b.ib = b.builder.Uint16DeltaBuilder(constants.ID)
	b.ib.SetMaxDelta(1)
	b.pib = b.builder.Uint16Builder(constants.ParentID)

	b.nb = b.builder.StringBuilder(constants.Name)
	b.db = b.builder.StringBuilder(constants.Description)
	b.ub = b.builder.StringBuilder(constants.Unit)
	b.atb = b.builder.Int32Builder(constants.AggregationTemporality)
	b.imb = b.builder.BooleanBuilder(constants.IsMonotonic)
	b.sab = acommon.AttributesBuilderFrom(b.builder.MapBuilder(constants.SharedAttributes))
	b.sstunb = b.builder.TimestampBuilder(constants.SharedStartTimeUnixNano)
	b.stunb = b.builder.TimestampBuilder(constants.SharedTimeUnixNano)
}

func (b *MetricBuilder) SetIntSumAccumulator(accumulator *IDPAccumulator) {
	b.intSumAccumulator = accumulator
}

func (b *MetricBuilder) SetDoubleSumAccumulator(accumulator *DDPAccumulator) {
	b.doubleSumAccumulator = accumulator
}

func (b *MetricBuilder) SetIntGaugeAccumulator(accumulator *IDPAccumulator) {
	b.intGaugeAccumulator = accumulator
}

func (b *MetricBuilder) SetDoubleGaugeAccumulator(accumulator *DDPAccumulator) {
	b.doubleGaugeAccumulator = accumulator
}

func (b *MetricBuilder) SetSummaryAccumulator(accumulator *SummaryAccumulator) {
	b.summaryAccumulator = accumulator
}

func (b *MetricBuilder) SetHistogramAccumulator(accumulator *HDPAccumulator) {
	b.histogramAccumulator = accumulator
}

func (b *MetricBuilder) SetEHistogramAccumulator(accumulator *EHDPAccumulator) {
	b.ehistogramAccumulator = accumulator
}

func (b *MetricBuilder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *MetricBuilder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *MetricBuilder) Accumulator() *MetricAccumulator {
	return b.accumulator
}

// Build builds the span array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *MetricBuilder) Build() (record arrow.Record, err error) {
	schemaNotUpToDateCount := 0

	// Loop until the record is built successfully.
	// Intermediaries steps may be required to update the schema.
	for {
		b.intSumAccumulator.Reset()
		b.intGaugeAccumulator.Reset()
		record, err = b.TryBuild()
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

func (b *MetricBuilder) TryBuild() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	b.accumulator.Sort()

	for ID, metric := range b.accumulator.metrics {
		b.ib.Append(uint16(ID))
		b.pib.Append(metric.ParentID)

		b.nb.AppendNonEmpty(metric.Name)
		b.db.AppendNonEmpty(metric.Description)
		b.ub.AppendNonEmpty(metric.Unit)

		aggrTempo, monotonic, err := b.AppendMetric(uint16(ID), metric.Metric, metric.SMData, metric.MData)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		b.atb.Append(int32(aggrTempo))
		b.imb.Append(monotonic)

		attrs := pcommon.NewMap()
		if metric.MData.Attributes != nil && metric.MData.Attributes.Len() > 0 {
			metric.MData.Attributes.CopyTo(attrs)
		}
		err = b.sab.Append(attrs)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		if metric.MData != nil && metric.MData.StartTime != nil {
			b.sstunb.Append(arrow.Timestamp(*metric.MData.StartTime))
		} else {
			b.sstunb.AppendNull()
		}

		if metric.MData != nil && metric.MData.Time != nil {
			b.stunb.Append(arrow.Timestamp(*metric.MData.Time))
		} else {
			b.stunb.AppendNull()
		}
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}
	return
}

func (b *MetricBuilder) AppendMetric(
	metricID uint16,
	metric *pmetric.Metric,
	smdata *ScopeMetricsSharedData,
	mdata *MetricSharedData,
) (aggrTempo pmetric.AggregationTemporality, monotonic bool, err error) {
	if b.released {
		return 0, false, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dps := metric.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			switch dp.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				b.intGaugeAccumulator.Append(metricID, dp)
			case pmetric.NumberDataPointValueTypeDouble:
				b.doubleGaugeAccumulator.Append(metricID, dp)
			default:
				panic(fmt.Sprintf("unknown value type %d", dp.ValueType()))
			}
		}
	case pmetric.MetricTypeSum:
		sum := metric.Sum()

		aggrTempo = sum.AggregationTemporality()
		monotonic = sum.IsMonotonic()

		dps := sum.DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			switch dp.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				b.intSumAccumulator.Append(metricID, dp)
			case pmetric.NumberDataPointValueTypeDouble:
				b.doubleSumAccumulator.Append(metricID, dp)
			default:
				panic(fmt.Sprintf("unknown value type %d", dp.ValueType()))
			}
		}
	case pmetric.MetricTypeSummary:
		err = b.summaryAccumulator.Append(metricID, metric.Summary().DataPoints())
		if err != nil {
			return
		}
	case pmetric.MetricTypeHistogram:
		histogram := metric.Histogram()
		aggrTempo = histogram.AggregationTemporality()

		err = b.histogramAccumulator.Append(metricID, histogram.DataPoints())
		if err != nil {
			return
		}
	case pmetric.MetricTypeExponentialHistogram:
		histogram := metric.ExponentialHistogram()
		aggrTempo = histogram.AggregationTemporality()

		err = b.ehistogramAccumulator.Append(metricID, histogram.DataPoints())
		if err != nil {
			return
		}
	case pmetric.MetricTypeEmpty:
		// ignore empty metric
	}

	return
}

func (b *MetricBuilder) Reset() {
	b.accumulator.Reset()
}

func (b *MetricBuilder) PayloadType() *acommon.PayloadType {
	return acommon.PayloadTypes.Metric
}

// Release releases the memory allocated by the builder.
func (b *MetricBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}

func NewMetricAccumulator() *MetricAccumulator {
	return &MetricAccumulator{
		groupCount: 0,
		metrics:    make([]Metric, 0),
	}
}

func (a *MetricAccumulator) IsEmpty() bool {
	return len(a.metrics) == 0
}

func (a *MetricAccumulator) Append(
	ParentID uint16,
	metric *pmetric.Metric,
	smdata *ScopeMetricsSharedData,
	mdata *MetricSharedData,
) error {
	if a.groupCount == math.MaxUint16 {
		panic("The maximum number of group of metric has been reached (max is uint16).")
	}

	a.metrics = append(a.metrics, Metric{
		ParentID:    ParentID,
		Name:        metric.Name(),
		Description: metric.Description(),
		Unit:        metric.Unit(),
		MetricType:  metric.Type(),
		Metric:      metric,
		SMData:      smdata,
		MData:       mdata,
	})

	a.groupCount++

	return nil
}

func (a *MetricAccumulator) Sort() {
	sort.Slice(a.metrics, func(i, j int) bool {
		if a.metrics[i].Name == a.metrics[j].Name {
			return a.metrics[i].ParentID < a.metrics[j].ParentID
		} else {
			return a.metrics[i].Name < a.metrics[j].Name
		}
	})
}

func (a *MetricAccumulator) Reset() {
	a.groupCount = 0
	a.metrics = a.metrics[:0]
}
