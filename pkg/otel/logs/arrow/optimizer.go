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
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/f5/otel-arrow-adapter/pkg/benchmark/stats"
	carrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/otlp"
)

type LogsOptimizer struct {
	sort  bool
	stats *stats.LogsStats
}

type LogsOptimized struct {
	ResourceLogs map[string]*ResourceLogGroup // resource logs id -> resource logs group
}

type ResourceLogGroup struct {
	Resource          *pcommon.Resource
	ResourceSchemaUrl string
	ScopeLogs         map[string]*ScopeLogGroup // scope logs id -> scope logs group
}

type ScopeLogGroup struct {
	Scope          *pcommon.InstrumentationScope
	ScopeSchemaUrl string

	Logs []*plog.LogRecord
}

func NewLogsOptimizer(cfg ...func(*carrow.Options)) *LogsOptimizer {
	options := carrow.Options{
		Sort:  false,
		Stats: false,
	}
	for _, c := range cfg {
		c(&options)
	}

	var s *stats.LogsStats
	if options.Stats {
		s = stats.NewLogsStats()
	}

	return &LogsOptimizer{
		sort:  options.Sort,
		stats: s,
	}
}

func (t *LogsOptimizer) Optimize(logs plog.Logs) *LogsOptimized {
	logsOptimized := &LogsOptimized{
		ResourceLogs: make(map[string]*ResourceLogGroup),
	}

	resLogsSlice := logs.ResourceLogs()
	for i := 0; i < resLogsSlice.Len(); i++ {
		resLogs := resLogsSlice.At(i)
		logsOptimized.AddResourceLogs(&resLogs)
	}

	if t.sort {
		for _, resLogGroup := range logsOptimized.ResourceLogs {
			resLogGroup.Sort()
		}
	}

	return logsOptimized
}

func (t *LogsOptimized) AddResourceLogs(resLogs *plog.ResourceLogs) {
	resLogsID := otlp.ResourceID(resLogs.Resource(), resLogs.SchemaUrl())
	resLogGroup, found := t.ResourceLogs[resLogsID]
	if !found {
		res := resLogs.Resource()
		resLogGroup = &ResourceLogGroup{
			Resource:          &res,
			ResourceSchemaUrl: resLogs.SchemaUrl(),
			ScopeLogs:         make(map[string]*ScopeLogGroup),
		}
		t.ResourceLogs[resLogsID] = resLogGroup
	}
	scopeLogsSlice := resLogs.ScopeLogs()
	for i := 0; i < scopeLogsSlice.Len(); i++ {
		scopeLogs := scopeLogsSlice.At(i)
		resLogGroup.AddScopeLogs(&scopeLogs)
	}
}

func (r *ResourceLogGroup) AddScopeLogs(scopeLogs *plog.ScopeLogs) {
	scopeLogID := otlp.ScopeID(scopeLogs.Scope(), scopeLogs.SchemaUrl())
	scopeLogGroup, found := r.ScopeLogs[scopeLogID]
	if !found {
		scope := scopeLogs.Scope()
		scopeLogGroup = &ScopeLogGroup{
			Scope:          &scope,
			ScopeSchemaUrl: scopeLogs.SchemaUrl(),
			Logs:           make([]*plog.LogRecord, 0),
		}
		r.ScopeLogs[scopeLogID] = scopeLogGroup
	}
	logsSlice := scopeLogs.LogRecords()
	for i := 0; i < logsSlice.Len(); i++ {
		log := logsSlice.At(i)
		scopeLogGroup.Logs = append(scopeLogGroup.Logs, &log)
	}
}

func (r *ResourceLogGroup) Sort() {
	for _, scopeLogGroup := range r.ScopeLogs {
		sort.Slice(scopeLogGroup.Logs, func(i, j int) bool {
			return strings.Compare(
				scopeLogGroup.Logs[i].TraceID().String(),
				scopeLogGroup.Logs[j].TraceID().String(),
			) == -1
		})
	}
}
