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

package datagen

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var cpuStates = []string{"idle", "user", "system", "iowait", "interrupt"}

type MetricsGenerator struct {
	*DataGenerator
	generation int
}

func NewMetricsGenerator(resourceAttributes []pcommon.Map, instrumentationScopes []pcommon.InstrumentationScope) *MetricsGenerator {
	return &MetricsGenerator{
		DataGenerator: NewDataGenerator(uint64(time.Now().UnixNano()/int64(time.Millisecond)), resourceAttributes, instrumentationScopes),
		generation:    0,
	}
}

func (mg *MetricsGenerator) newResult() (pmetric.Metrics, pmetric.MetricSlice) {
	result := pmetric.NewMetrics()

	resourceMetrics := result.ResourceMetrics().AppendEmpty()
	mg.resourceAttributes[mg.generation%len(mg.resourceAttributes)].
		CopyTo(resourceMetrics.Resource().Attributes())
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	mg.instrumentationScopes[mg.generation%len(mg.instrumentationScopes)].
		CopyTo(scopeMetrics.Scope())
	return result, scopeMetrics.Metrics()
}

func (mg *MetricsGenerator) Generate(batchSize int, collectInterval time.Duration) pmetric.Metrics {
	result, metrics := mg.newResult()

	// Note: the OTLP data model calls for aggregation of the
	// points, this is repeating metrics.  nevertheless, the
	// conversion to Arrow should handle this case.
	for i := 0; i < batchSize; i++ {
		mg.AdvanceTime(collectInterval)

		mg.SystemCpuTime(metrics.AppendEmpty(), 1)
		mg.SystemMemoryUsage(metrics.AppendEmpty())
		mg.SystemCpuLoadAverage1m(metrics.AppendEmpty())
		mg.HypotheticalHistogram(metrics.AppendEmpty())
	}

	mg.generation++

	return result
}

func (mg *MetricsGenerator) GenerateSystemCpuTime(batchSize int, collectInterval time.Duration) pmetric.Metrics {
	result, metrics := mg.newResult()

	for i := 0; i < batchSize; i++ {
		mg.AdvanceTime(collectInterval)

		mg.SystemCpuTime(metrics.AppendEmpty(), 1)
	}

	mg.generation++

	return result
}

func (mg *MetricsGenerator) GenerateSystemMemoryUsage(batchSize int, collectInterval time.Duration) pmetric.Metrics {
	result, metrics := mg.newResult()

	for i := 0; i < batchSize; i++ {
		mg.AdvanceTime(collectInterval)

		mg.SystemMemoryUsage(metrics.AppendEmpty())
	}
	mg.generation++

	return result
}

func (mg *MetricsGenerator) GenerateSystemCpuLoadAverage1m(batchSize int, collectInterval time.Duration) pmetric.Metrics {
	result, metrics := mg.newResult()

	for i := 0; i < batchSize; i++ {
		mg.AdvanceTime(collectInterval)

		mg.SystemCpuLoadAverage1m(metrics.AppendEmpty())
	}

	mg.generation++

	return result
}

func (dg *DataGenerator) SystemCpuTime(metric pmetric.Metric, cpuCount int) {
	metric.SetName("system.cpu.time")
	metric.SetUnit("s")

	sum := metric.SetEmptySum()
	//sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	//sum.SetIsMonotonic(true)
	points := sum.DataPoints()

	for cpu := 0; cpu < cpuCount; cpu++ {
		for _, state := range cpuStates {
			dataPoint := points.AppendEmpty()
			dataPoint.Attributes().PutStr("state", state)
			dataPoint.Attributes().PutInt("cpu", int64(cpu))
			dataPoint.SetStartTimestamp(dg.PrevTime())
			dataPoint.SetTimestamp(dg.CurrentTime())
			dataPoint.SetDoubleValue(dg.GenF64Range(0.0, 1.0))
		}
	}
}

func (dg *DataGenerator) SystemMemoryUsage(metric pmetric.Metric) {
	metric.SetName("system.memory.usage")
	metric.SetDescription("Bytes of memory in use.")
	metric.SetUnit("By")
	sum := metric.SetEmptySum()
	//sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	//sum.SetIsMonotonic(false)
	points := sum.DataPoints()

	p1 := points.AppendEmpty()
	p1.Attributes().PutStr("state", "used")
	p1.SetStartTimestamp(dg.PrevTime())
	p1.SetTimestamp(dg.CurrentTime())
	p1.SetIntValue(dg.GenI64Range(10_000_000_000, 13_000_000_000))

	p2 := points.AppendEmpty()
	p2.Attributes().PutStr("state", "free")
	p2.SetStartTimestamp(dg.PrevTime())
	p2.SetTimestamp(dg.CurrentTime())
	p2.SetIntValue(dg.GenI64Range(300_000_000, 500_000_000))

	p3 := points.AppendEmpty()
	p3.Attributes().PutStr("state", "inactive")
	p3.SetStartTimestamp(dg.PrevTime())
	p3.SetTimestamp(dg.CurrentTime())
	p3.SetIntValue(4_000_000_000)
}

func (dg *DataGenerator) SystemCpuLoadAverage1m(metric pmetric.Metric) {
	metric.SetName("system.cpu.load_average.1m")
	metric.SetDescription("Average CPU Load over 1 minute.")
	metric.SetUnit("1")

	point := metric.SetEmptyGauge().DataPoints().AppendEmpty()

	point.SetStartTimestamp(dg.PrevTime())
	point.SetTimestamp(dg.CurrentTime())
	point.SetDoubleValue(dg.GenF64Range(1.0, 100.0))
}

func (dg *DataGenerator) HypotheticalHistogram(metric pmetric.Metric) {
	metric.SetName("hypothetical.histogram")
	metric.SetDescription("A histogram with a few buckets.")
	metric.SetUnit("1")

	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dps := histogram.DataPoints()
	dps.EnsureCapacity(10)

	for i := 0; i < 10; i++ {
		dp := dps.AppendEmpty()
		dp.SetStartTimestamp(dg.PrevTime())
		dp.SetTimestamp(dg.CurrentTime())
		dp.SetCount(uint64(dg.GenI64Range(0, 100)))
		dp.SetSum(dg.GenF64Range(0, 100))

		bcs := dp.BucketCounts()
		bcs.EnsureCapacity(10)
		for j := 0; j < 10; j++ {
			bcs.Append(uint64(dg.GenI64Range(0, 100)))
		}

		ebs := dp.ExplicitBounds()
		ebs.EnsureCapacity(10)
		for j := 0; j < 10; j++ {
			ebs.Append(dg.GenF64Range(0, 100))
		}
		dp.SetFlags(pmetric.DataPointFlags(dg.GenI64Range(1, 50)))
		dp.SetMin(dg.GenF64Range(0, 100))
		dp.SetMax(dg.GenF64Range(0, 100))
	}
}
