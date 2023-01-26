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

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dustin/go-humanize"

	"github.com/f5/otel-arrow-adapter/pkg/benchmark"
	"github.com/f5/otel-arrow-adapter/pkg/benchmark/dataset"
	"github.com/f5/otel-arrow-adapter/pkg/benchmark/profileable/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/benchmark/profileable/otlp"
)

var help = flag.Bool("help", false, "Show help")

func main() {
	// Parse the flag
	flag.Parse()

	// Usage Demo
	if *help {
		flag.Usage()
		os.Exit(0)
	}

	// Define default input file
	inputFiles := flag.Args()
	if len(inputFiles) == 0 {
		inputFiles = append(inputFiles, "./data/otlp_traces.pb")
	}

	// Compare the performance for each input file
	for i := range inputFiles {
		// Compare the performance between the standard OTLP representation and the OTLP Arrow representation.
		profiler := benchmark.NewProfiler([]int{ /*10, 100, */ 1000, 2000, 5000, 10000}, "output/trace_benchmark.log", 2)
		compressionAlgo := benchmark.Zstd()
		maxIter := uint64(1)
		ds := dataset.NewRealTraceDataset(inputFiles[i], []string{"trace_id"})
		profiler.Printf("Dataset '%s' (%s) loaded\n", inputFiles[i], humanize.Bytes(uint64(ds.SizeInBytes())))
		otlpTraces := otlp.NewTraceProfileable(ds, compressionAlgo)

		conf := &benchmark.Config{}
		otlpArrowTraces := arrow.NewTraceProfileable([]string{}, ds, conf)

		if err := profiler.Profile(otlpTraces, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}

		if err := profiler.Profile(otlpArrowTraces, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}

		profiler.CheckProcessingResults()

		// Configure the profile output
		benchmark.OtlpArrowConversionSection.CustomColumnFor(otlpTraces).
			MetricNotApplicable()

		profiler.Printf("\nTraces dataset summary:\n")
		profiler.Printf("- #traces: %d\n", ds.Len())
		profiler.Printf("- size: %s\n", humanize.Bytes(uint64(ds.SizeInBytes())))

		profiler.PrintResults(maxIter)

		profiler.ExportMetricsTimesCSV(fmt.Sprintf("%d_traces_benchmark_results", i))
		profiler.ExportMetricsBytesCSV(fmt.Sprintf("%d_traces_benchmark_results", i))
	}
}
