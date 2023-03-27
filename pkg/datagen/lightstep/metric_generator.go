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

package lightstep

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep/topology"
)

type MetricGenerator struct {
	metricCount int
}

func NewMetricGenerator() *MetricGenerator {
	return &MetricGenerator{
		metricCount: 0,
	}
}

func (g *MetricGenerator) Generate(metric *topology.Metric, serviceName string) (pmetric.Metrics, bool) {
	metrics := pmetric.NewMetrics()

	if !metric.ShouldGenerate() {
		return metrics, false
	}

	rms := metrics.ResourceMetrics().AppendEmpty()
	rms.Resource().Attributes().PutStr("service.name", serviceName)

	m := rms.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(metric.Name)
	if metric.Type == "Gauge" {
		m.SetEmptyGauge()
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetDoubleValue(metric.GetValue())
		for k, v := range metric.GetTags() {
			dp.Attributes().PutStr(k, v)
		}
	} else if metric.Type == "Sum" {
		// TODO: support int-type values
		// TODO: support cumulative?
		m.SetEmptySum()
		m.Sum().SetIsMonotonic(true)
		m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		dp := m.Sum().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetDoubleValue(metric.GetValue())
		for k, v := range metric.GetTags() {
			dp.Attributes().PutStr(k, v)
		}
	}
	// TODO: support histograms!

	g.metricCount = g.metricCount + 1
	return metrics, true
}
