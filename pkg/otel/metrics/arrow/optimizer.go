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
	"sort"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/f5/otel-arrow-adapter/pkg/benchmark/stats"
	carrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/otlp"
)

type MetricsOptimizer struct {
	sort  bool
	stats *stats.MetricsStats
}

type MetricsOptimized struct {
	ResourceMetrics map[string]*ResourceMetricsGroup // resource metrics id -> resource metrics group
}

type ResourceMetricsGroup struct {
	Resource          *pcommon.Resource
	ResourceSchemaUrl string
	ScopeMetrics      map[string]*ScopeMetricsGroup // scope metrics id -> scope metrics group
}

type ScopeMetricsGroup struct {
	Scope          *pcommon.InstrumentationScope
	ScopeSchemaUrl string

	Metrics []*pmetric.Metric
}

func NewMetricsOptimizer(cfg ...func(*carrow.Options)) *MetricsOptimizer {
	options := carrow.Options{
		Sort:  false,
		Stats: false,
	}
	for _, c := range cfg {
		c(&options)
	}

	var s *stats.MetricsStats
	if options.Stats {
		s = stats.NewMetricsStats()
	}

	return &MetricsOptimizer{
		sort:  options.Sort,
		stats: s,
	}
}

func (t *MetricsOptimizer) Optimize(metrics pmetric.Metrics) *MetricsOptimized {
	metricsOptimized := &MetricsOptimized{
		ResourceMetrics: make(map[string]*ResourceMetricsGroup),
	}

	resMetricsSlice := metrics.ResourceMetrics()
	for i := 0; i < resMetricsSlice.Len(); i++ {
		resMetrics := resMetricsSlice.At(i)
		metricsOptimized.AddResourceMetrics(&resMetrics)
	}

	if t.sort {
		for _, resMetricsGroup := range metricsOptimized.ResourceMetrics {
			resMetricsGroup.Sort()
		}
	}

	return metricsOptimized
}

func (t *MetricsOptimized) AddResourceMetrics(resMetrics *pmetric.ResourceMetrics) {
	resMetricsID := otlp.ResourceID(resMetrics.Resource(), resMetrics.SchemaUrl())
	resMetricsGroup, found := t.ResourceMetrics[resMetricsID]
	if !found {
		res := resMetrics.Resource()
		resMetricsGroup = &ResourceMetricsGroup{
			Resource:          &res,
			ResourceSchemaUrl: resMetrics.SchemaUrl(),
			ScopeMetrics:      make(map[string]*ScopeMetricsGroup),
		}
		t.ResourceMetrics[resMetricsID] = resMetricsGroup
	}
	scopeMetricsSlice := resMetrics.ScopeMetrics()
	for i := 0; i < scopeMetricsSlice.Len(); i++ {
		scopeMetrics := scopeMetricsSlice.At(i)
		resMetricsGroup.AddScopeMetrics(&scopeMetrics)
	}
}

func (r *ResourceMetricsGroup) AddScopeMetrics(scopeMetrics *pmetric.ScopeMetrics) {
	scopeMetricsID := otlp.ScopeID(scopeMetrics.Scope(), scopeMetrics.SchemaUrl())
	scopeMetricsGroup, found := r.ScopeMetrics[scopeMetricsID]
	if !found {
		scope := scopeMetrics.Scope()
		scopeMetricsGroup = &ScopeMetricsGroup{
			Scope:          &scope,
			ScopeSchemaUrl: scopeMetrics.SchemaUrl(),
			Metrics:        make([]*pmetric.Metric, 0),
		}
		r.ScopeMetrics[scopeMetricsID] = scopeMetricsGroup
	}
	metricsSlice := scopeMetrics.Metrics()
	for i := 0; i < metricsSlice.Len(); i++ {
		metric := metricsSlice.At(i)
		scopeMetricsGroup.Metrics = append(scopeMetricsGroup.Metrics, &metric)
	}
}

func (r *ResourceMetricsGroup) Sort() {
	for _, scopeMetricsGroup := range r.ScopeMetrics {
		sort.Slice(scopeMetricsGroup.Metrics, func(i, j int) bool {
			return strings.Compare(
				scopeMetricsGroup.Metrics[i].Name(),
				scopeMetricsGroup.Metrics[j].Name(),
			) == -1
		})
	}
}
