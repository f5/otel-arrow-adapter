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
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// ScopeMetricsDT is the Arrow Data Type describing a scope span.
var (
	ScopeMetricsDT = arrow.StructOf([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint16, Metadata: schema.Metadata(schema.DeltaEncoding)},
		{Name: constants.Scope, Type: acommon.ScopeDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SchemaUrl, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.SharedAttributes, Type: acommon.AttributesDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SharedStartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SharedTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
	}...)
)

// ScopeMetricsBuilder is a helper to build a scope spans.
type ScopeMetricsBuilder struct {
	released bool

	builder *builder.StructBuilder

	ib     *builder.Uint16DeltaBuilder // id builder
	scb    *acommon.ScopeBuilder       // scope builder
	schb   *builder.StringBuilder      // schema url builder
	sab    *acommon.AttributesBuilder  // shared attributes builder
	sstunb *builder.TimestampBuilder   // shared start time unix nano builder
	stunb  *builder.TimestampBuilder   // shared time unix nano builder
}

type DataPoint interface {
	Attributes() pcommon.Map
	Timestamp() pcommon.Timestamp
	StartTimestamp() pcommon.Timestamp
}

func ScopeMetricsBuilderFrom(builder *builder.StructBuilder) *ScopeMetricsBuilder {
	ib := builder.Uint16DeltaBuilder(constants.ID)
	ib.SetMaxDelta(1)

	return &ScopeMetricsBuilder{
		released: false,
		builder:  builder,
		ib:       ib,
		scb:      acommon.ScopeBuilderFrom(builder.StructBuilder(constants.Scope)),
		schb:     builder.StringBuilder(constants.SchemaUrl),
		sab:      acommon.AttributesBuilderFrom(builder.MapBuilder(constants.SharedAttributes)),
		sstunb:   builder.TimestampBuilder(constants.SharedStartTimeUnixNano),
		stunb:    builder.TimestampBuilder(constants.SharedTimeUnixNano),
	}
}

// Build builds the scope metrics array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ScopeMetricsBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Append appends a new scope metrics to the builder.
func (b *ScopeMetricsBuilder) Append(smg *ScopeMetricsGroup, relatedData *RelatedData) error {
	if b.released {
		return werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	return b.builder.Append(smg, func() error {
		ID := relatedData.NextMetricScopeID()

		b.ib.Append(ID)
		if err := b.scb.Append(smg.Scope, relatedData.AttrsBuilders().scope.Accumulator()); err != nil {
			return werror.Wrap(err)
		}
		b.schb.AppendNonEmpty(smg.ScopeSchemaUrl)

		sharedData, err := NewMetricsSharedData(smg.Metrics)
		if err != nil {
			return werror.Wrap(err)
		}

		for i, metric := range smg.Metrics {
			if err := relatedData.MetricsBuilder().Accumulator().Append(ID, metric, sharedData, sharedData.Metrics[i]); err != nil {
				return werror.Wrap(err)
			}
		}

		if sharedData.Attributes != nil && sharedData.Attributes.Len() > 0 {
			attrs := pcommon.NewMap()
			sharedData.Attributes.CopyTo(attrs)
			err = b.sab.Append(attrs)
			if err != nil {
				return werror.Wrap(err)
			}
		} else {
			// if no shared attributes
			if err := b.sab.AppendNull(); err != nil {
				return werror.Wrap(err)
			}
		}

		if sharedData.StartTime != nil {
			b.sstunb.Append(arrow.Timestamp(*sharedData.StartTime))
		} else {
			b.sstunb.AppendNull()
		}

		if sharedData.Time != nil {
			b.stunb.Append(arrow.Timestamp(*sharedData.Time))
		} else {
			b.stunb.AppendNull()
		}

		return nil
	})
}

// Release releases the memory allocated by the builder.
func (b *ScopeMetricsBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}

type ScopeMetricsSharedData struct {
	StartTime  *pcommon.Timestamp
	Time       *pcommon.Timestamp
	Attributes *common.SharedAttributes
	Metrics    []*MetricSharedData
}

type MetricSharedData struct {
	// number of data points
	NumDP      int
	StartTime  *pcommon.Timestamp
	Time       *pcommon.Timestamp
	Attributes *common.SharedAttributes
}

func NewMetricsSharedData(metrics []*pmetric.Metric) (sharedData *ScopeMetricsSharedData, err error) {
	sharedData = &ScopeMetricsSharedData{Metrics: make([]*MetricSharedData, len(metrics))}

	if len(metrics) > 0 {
		msd, err := NewMetricSharedData(metrics[0])
		if err != nil {
			return nil, werror.Wrap(err)
		}
		sharedData.StartTime = msd.StartTime
		sharedData.Time = msd.Time
		sharedData.Attributes = msd.Attributes.Clone()
		sharedData.Metrics[0] = msd
	}
	for i := 1; i < len(metrics); i++ {
		msd, err := NewMetricSharedData(metrics[i])
		if err != nil {
			return nil, werror.Wrap(err)
		}
		sharedData.Metrics[i] = msd
		if msd.StartTime != nil && sharedData.StartTime != nil && uint64(*sharedData.StartTime) != uint64(*msd.StartTime) {
			sharedData.StartTime = nil
		}
		if msd.Time != nil && sharedData.Time != nil && uint64(*sharedData.Time) != uint64(*msd.Time) {
			sharedData.Time = nil
		}
		if sharedData.Attributes.Len() > 0 {
			sharedData.Attributes.IntersectWith(msd.Attributes)
		}
	}
	if sharedData != nil {
		if sharedData.StartTime != nil {
			for i := 0; i < len(sharedData.Metrics); i++ {
				sharedData.Metrics[i].StartTime = nil
			}
		}
		if sharedData.Time != nil {
			for i := 0; i < len(sharedData.Metrics); i++ {
				sharedData.Metrics[i].Time = nil
			}
		}
		for k := range sharedData.Attributes.Attributes {
			for i := 0; i < len(sharedData.Metrics); i++ {
				delete(sharedData.Metrics[i].Attributes.Attributes, k)
			}
		}
		if sharedData.Attributes.Len() == 0 {
			sharedData.Attributes = nil
		}
	}
	return sharedData, err
}

func NewMetricSharedData(metric *pmetric.Metric) (sharedData *MetricSharedData, err error) {
	sharedData = &MetricSharedData{}
	var dpLen func() int
	var dpAt func(int) DataPoint

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dps := metric.Gauge().DataPoints()
		dpLen = func() int { return dps.Len() }
		dpAt = func(i int) DataPoint { return dps.At(i) }
	case pmetric.MetricTypeSum:
		dps := metric.Sum().DataPoints()
		dpLen = func() int { return dps.Len() }
		dpAt = func(i int) DataPoint { return dps.At(i) }
	case pmetric.MetricTypeHistogram:
		dps := metric.Histogram().DataPoints()
		dpLen = func() int { return dps.Len() }
		dpAt = func(i int) DataPoint { return dps.At(i) }
	case pmetric.MetricTypeSummary:
		dps := metric.Summary().DataPoints()
		dpLen = func() int { return dps.Len() }
		dpAt = func(i int) DataPoint { return dps.At(i) }
	case pmetric.MetricTypeExponentialHistogram:
		dps := metric.ExponentialHistogram().DataPoints()
		dpLen = func() int { return dps.Len() }
		dpAt = func(i int) DataPoint { return dps.At(i) }
	case pmetric.MetricTypeEmpty:
		// ignore empty metric.
	default:
		err = werror.Wrap(ErrUnknownMetricType)
		return sharedData, err
	}

	sharedData.NumDP = dpLen()
	if sharedData.NumDP > 0 {
		initSharedDataFrom(sharedData, dpAt(0))
		for i := 1; i < sharedData.NumDP; i++ {
			updateSharedDataWith(sharedData, dpAt(i))
		}
	}

	return sharedData, err
}

func initSharedDataFrom(sharedData *MetricSharedData, initDataPoint DataPoint) {
	//startTime := initDataPoint.StartTimestamp()
	sharedData.StartTime = nil // &startTime
	//time := initDataPoint.Timestamp()
	sharedData.Time = nil // &time
	sharedData.Attributes = common.NewSharedAttributesFrom( /*initDataPoint.Attributes()*/ pcommon.NewMap())
}

func updateSharedDataWith(sharedData *MetricSharedData, dp DataPoint) int {
	if sharedData.StartTime != nil && uint64(*sharedData.StartTime) != uint64(dp.StartTimestamp()) {
		sharedData.StartTime = nil
	}
	if sharedData.Time != nil && uint64(*sharedData.Time) != uint64(dp.Timestamp()) {
		sharedData.Time = nil
	}
	return sharedData.Attributes.IntersectWithMap(dp.Attributes())
}
