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
	"go.opentelemetry.io/collector/pdata/pmetric"

	arrow2 "github.com/f5/otel-arrow-adapter/pkg/arrow"
	carrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/otel/stats"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// MetricsSchema is the Arrow schema for the OTLP Arrow Metrics record.
var (
	MetricsSchema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint16, Metadata: schema.Metadata(schema.DeltaEncoding)},
		{Name: constants.Resource, Type: carrow.ResourceDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Scope, Type: carrow.ScopeDT, Metadata: schema.Metadata(schema.Optional)},
		// This schema URL applies to the span and span events (the schema URL
		// for the resource is in the resource struct).
		{Name: constants.SchemaUrl, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.MetricType, Type: arrow.PrimitiveTypes.Uint8},
		{Name: constants.Name, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.Description, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.Unit, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.AggregationTemporality, Type: arrow.PrimitiveTypes.Int32, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.IsMonotonic, Type: arrow.FixedWidthTypes.Boolean, Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

// MetricsBuilder is a helper to build a list of resource metrics.
type MetricsBuilder struct {
	released bool

	builder *builder.RecordBuilderExt   // Record builder
	rb      *carrow.ResourceBuilder     // `resource` builder
	scb     *carrow.ScopeBuilder        // `scope` builder
	sschb   *builder.StringBuilder      // scope `schema_url` builder
	ib      *builder.Uint16DeltaBuilder //  id builder
	mtb     *builder.Uint8Builder       // metric type builder
	nb      *builder.StringBuilder      // metric name builder
	db      *builder.StringBuilder      // metric description builder
	ub      *builder.StringBuilder      // metric unit builder
	atb     *builder.Int32Builder       // aggregation temporality builder
	imb     *builder.BooleanBuilder     // is monotonic builder

	optimizer *MetricsOptimizer
	analyzer  *MetricsAnalyzer

	relatedData *RelatedData
}

// NewMetricsBuilder creates a new MetricsBuilder with a given allocator.
func NewMetricsBuilder(
	rBuilder *builder.RecordBuilderExt,
	cfg *Config,
	stats *stats.ProducerStats,
) (*MetricsBuilder, error) {
	var optimizer *MetricsOptimizer
	var analyzer *MetricsAnalyzer

	relatedData, err := NewRelatedData(cfg, stats)
	if err != nil {
		panic(err)
	}

	if stats.SchemaStatsEnabled {
		optimizer = NewMetricsOptimizer(cfg.Metric.Sorter)
		analyzer = NewMetricsAnalyzer()
	} else {
		optimizer = NewMetricsOptimizer(cfg.Metric.Sorter)
	}

	b := &MetricsBuilder{
		released:    false,
		builder:     rBuilder,
		optimizer:   optimizer,
		analyzer:    analyzer,
		relatedData: relatedData,
	}

	if err := b.init(); err != nil {
		return nil, werror.Wrap(err)
	}

	return b, nil
}

func (b *MetricsBuilder) init() error {
	b.ib = b.builder.Uint16DeltaBuilder(constants.ID)
	// As metrics are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	b.ib.SetMaxDelta(1)

	b.rb = carrow.ResourceBuilderFrom(b.builder.StructBuilder(constants.Resource))
	b.scb = carrow.ScopeBuilderFrom(b.builder.StructBuilder(constants.Scope))
	b.sschb = b.builder.StringBuilder(constants.SchemaUrl)

	b.mtb = b.builder.Uint8Builder(constants.MetricType)
	b.nb = b.builder.StringBuilder(constants.Name)
	b.db = b.builder.StringBuilder(constants.Description)
	b.ub = b.builder.StringBuilder(constants.Unit)
	b.atb = b.builder.Int32Builder(constants.AggregationTemporality)
	b.imb = b.builder.BooleanBuilder(constants.IsMonotonic)

	return nil
}

func (b *MetricsBuilder) RelatedData() *RelatedData {
	return b.relatedData
}

// Build builds an Arrow Record from the builder.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the record.
func (b *MetricsBuilder) Build() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(carrow.ErrBuilderAlreadyReleased)
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		initErr := b.init()
		if initErr != nil {
			err = werror.Wrap(initErr)
		}
	}

	// ToDo Keep this code for debugging purposes.
	if err == nil && count == 0 {
		println("Metrics")
		arrow2.PrintRecord(record)
		count = count + 1
	}

	return
}

// ToDo Keep this code for debugging purposes.
var count = 0

// Append appends a new set of resource metrics to the builder.
func (b *MetricsBuilder) Append(metrics pmetric.Metrics) error {
	if b.released {
		return werror.Wrap(carrow.ErrBuilderAlreadyReleased)
	}

	optimizedMetrics := b.optimizer.Optimize(metrics)
	if b.analyzer != nil {
		b.analyzer.Analyze(optimizedMetrics)
		b.analyzer.ShowStats("")
	}

	metricID := uint16(0)
	var resMetricsID, scopeMetricsID string
	var resID, scopeID int64
	var err error

	for _, metric := range optimizedMetrics.Metrics {
		ID := metricID

		b.ib.Append(ID)
		metricID++

		// Resource spans
		if resMetricsID != metric.ResourceMetricsID {
			resMetricsID = metric.ResourceMetricsID
			resID, err = b.relatedData.AttrsBuilders().Resource().Accumulator().Append(metric.Resource.Attributes())
			if err != nil {
				return werror.Wrap(err)
			}
		}
		if err = b.rb.AppendWithID(resID, metric.Resource, metric.ResourceSchemaUrl); err != nil {
			return werror.Wrap(err)
		}

		// Scope spans
		if scopeMetricsID != metric.ScopeMetricsID {
			scopeMetricsID = metric.ScopeMetricsID
			scopeID, err = b.relatedData.AttrsBuilders().scope.Accumulator().Append(metric.Scope.Attributes())
			if err != nil {
				return werror.Wrap(err)
			}
		}
		if err = b.scb.AppendWithAttrsID(scopeID, metric.Scope); err != nil {
			return werror.Wrap(err)
		}
		b.sschb.AppendNonEmpty(metric.ScopeSchemaUrl)

		// Metric type is an int32 in the proto spec, but we don't expect more
		// than 256 types, so we use an uint8 instead.
		b.mtb.Append(uint8(metric.Metric.Type()))
		b.nb.AppendNonEmpty(metric.Metric.Name())
		b.db.AppendNonEmpty(metric.Metric.Description())
		b.ub.AppendNonEmpty(metric.Metric.Unit())

		switch metric.Metric.Type() {
		case pmetric.MetricTypeGauge:
			b.atb.AppendNull()
			b.imb.AppendNull()
			dps := metric.Metric.Gauge().DataPoints()
			for i := 0; i < dps.Len(); i++ {
				dp := dps.At(i)
				switch dp.ValueType() {
				case pmetric.NumberDataPointValueTypeInt:
					b.relatedData.GaugeIDPBuilder().Accumulator().Append(ID, &dp)
				case pmetric.NumberDataPointValueTypeDouble:
					b.relatedData.GaugeDDPBuilder().Accumulator().Append(ID, &dp)
				}
			}
		case pmetric.MetricTypeSum:
			sum := metric.Metric.Sum()
			b.atb.Append(int32(sum.AggregationTemporality()))
			b.imb.Append(sum.IsMonotonic())
		case pmetric.MetricTypeSummary:
			b.atb.AppendNull()
			b.imb.AppendNull()
		case pmetric.MetricTypeHistogram:
			histogram := metric.Metric.Histogram()
			b.atb.Append(int32(histogram.AggregationTemporality()))
			b.imb.AppendNull()
		case pmetric.MetricTypeExponentialHistogram:
			exponentialHistogram := metric.Metric.ExponentialHistogram()
			b.atb.Append(int32(exponentialHistogram.AggregationTemporality()))
			b.imb.AppendNull()
		default:
			// ToDo should ignore unknow metric type and log.
		}
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *MetricsBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.released = true

		b.relatedData.Release()
	}
}

func (b *MetricsBuilder) ShowSchema() {
	b.builder.ShowSchema()
}
