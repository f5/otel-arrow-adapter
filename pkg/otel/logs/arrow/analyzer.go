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
	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/axiomhq/hyperloglog"

	carrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
)

// A log analyzer is a tool designed to generate statistics about the structure
// and content distribution of a stream of OpenTelemetry Protocol (OTLP) logs.
// By using the -stats flag in the benchmark tool, the results of this analysis
// can be conveniently displayed on the console to troubleshoot compression
// ratio issues.

type (
	LogsAnalyzer struct {
		LogRecordCount    int64
		ResourceLogsStats *ResourceLogsStats
	}

	ResourceLogsStats struct {
		TotalCount         int64
		Distribution       *hdrhistogram.Histogram
		ResLogsIDsDistinct *hyperloglog.Sketch
		ResourceStats      *carrow.ResourceStats
		ScopeSpansStats    *ScopeLogsStats
		SchemaUrlStats     *carrow.SchemaUrlStats
	}

	ScopeLogsStats struct {
		Distribution         *hdrhistogram.Histogram
		ScopeLogsIDsDistinct *hyperloglog.Sketch
		ScopeStats           *carrow.ScopeStats
		SchemaUrlStats       *carrow.SchemaUrlStats
		LogRecordsStats      *LogRecordsStats
	}

	LogRecordsStats struct {
		TotalCount           int64
		Distribution         *hdrhistogram.Histogram
		TimeUnixNano         *carrow.TimestampStats
		ObservedTimeUnixNano *carrow.TimestampStats
		TraceID              *hyperloglog.Sketch
		SpanID               *hyperloglog.Sketch
		SeverityNumber       *hyperloglog.Sketch
		SeverityText         *carrow.StringStats
		Body                 *carrow.AnyValueStats
		Attributes           *carrow.AttributesStats
		StatusStats          *carrow.StatusStats
	}
)

func NewLogsAnalyzer() *LogsAnalyzer {
	return &LogsAnalyzer{
		// ToDo
	}
}

func (t *LogsAnalyzer) Analyze(logs *LogsOptimized) {
	t.LogRecordCount++
	// ToDo
}

func (t *LogsAnalyzer) ShowStats(indent string) {
	// ToDo
}
