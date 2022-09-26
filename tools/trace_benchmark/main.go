/*
 * // Copyright The OpenTelemetry Authors
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //       http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 *
 */

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dustin/go-humanize"

	"otel-arrow-adapter/pkg/air/config"
	"otel-arrow-adapter/pkg/benchmark"
	"otel-arrow-adapter/pkg/benchmark/dataset"
	"otel-arrow-adapter/pkg/benchmark/profileable/otlp"
	"otel-arrow-adapter/pkg/benchmark/profileable/otlp_arrow"
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
		//profiler := benchmark.NewProfiler([]int{1000, 5000, 10000, 25000})
		profiler := benchmark.NewProfiler([]int{1000, 2000}, "output/trace_benchmark.log")
		compressionAlgo := benchmark.Zstd()
		maxIter := uint64(1)
		ds := dataset.NewRealTraceDataset(inputFiles[i], []string{"trace_id"})
		profiler.Printf("Dataset '%s' (%s) loaded\n", inputFiles[i], humanize.Bytes(uint64(ds.SizeInBytes())))
		otlpTraces := otlp.NewTraceProfileable(ds, compressionAlgo)

		conf := config.NewUint16DefaultConfig()
		conf.TraceEncoding = config.Flat
		conf.Attribute.Encoding = config.AttributesAsStructs
		otlpArrowTracesWithUint16Dictionary := otlp_arrow.NewTraceProfileable([]string{"uint16 dict", "attrs_as_structs"}, ds, conf, compressionAlgo)
		conf = config.NewUint16DefaultConfig()
		conf.TraceEncoding = config.Flat
		conf.Attribute.Encoding = config.AttributesAsListStructs
		otlpArrowTracesWithUint16DictionaryAsListStructs := otlp_arrow.NewTraceProfileable([]string{"uint16 dict", "attrs_as_list_structs"}, ds, conf, compressionAlgo)

		conf = config.NewUint16DefaultConfig()
		conf.TraceEncoding = config.Hierarchical
		conf.Attribute.Encoding = config.AttributesAsStructs
		otlpArrowTracesWithUint16DictionaryHierarchical := otlp_arrow.NewTraceProfileable([]string{"uint16 dict", "attrs_as_structs", "hierarchical"}, ds, conf, compressionAlgo)
		conf = config.NewUint16DefaultConfig()
		conf.TraceEncoding = config.Hierarchical
		conf.Attribute.Encoding = config.AttributesAsListStructs
		otlpArrowTracesWithUint16DictionaryAsListStructsHierarchical := otlp_arrow.NewTraceProfileable([]string{"uint16 dict", "attrs_as_list_structs", "hierarchical"}, ds, conf, compressionAlgo)

		conf = config.NewUint16DefaultConfig()
		conf.TraceEncoding = config.Hybrid
		conf.Attribute.Encoding = config.AttributesAsStructs
		otlpArrowTracesWithUint16DictionaryHybrid := otlp_arrow.NewTraceProfileable([]string{"uint16 dict", "attrs_as_structs", "hybrid"}, ds, conf, compressionAlgo)
		conf = config.NewUint16DefaultConfig()
		conf.TraceEncoding = config.Hybrid
		conf.Attribute.Encoding = config.AttributesAsListStructs
		otlpArrowTracesWithUint16DictionaryAsListStructsHybrid := otlp_arrow.NewTraceProfileable([]string{"uint16 dict", "attrs_as_list_structs", "hybrid"}, ds, conf, compressionAlgo)

		if err := profiler.Profile(otlpTraces, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}

		if err := profiler.Profile(otlpArrowTracesWithUint16Dictionary, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}
		if err := profiler.Profile(otlpArrowTracesWithUint16DictionaryAsListStructs, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}

		if err := profiler.Profile(otlpArrowTracesWithUint16DictionaryHierarchical, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}
		if err := profiler.Profile(otlpArrowTracesWithUint16DictionaryAsListStructsHierarchical, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}

		if err := profiler.Profile(otlpArrowTracesWithUint16DictionaryHybrid, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}
		if err := profiler.Profile(otlpArrowTracesWithUint16DictionaryAsListStructsHybrid, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}

		profiler.CheckProcessingResults()
		profiler.PrintResults(maxIter)

		profiler.ExportMetricsTimesCSV(fmt.Sprintf("%d_traces_benchmark_results", i))
		profiler.ExportMetricsBytesCSV(fmt.Sprintf("%d_traces_benchmark_results", i))
	}
}
