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
	"log"
	"os"
	"path"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/f5/otel-arrow-adapter/pkg/benchmark"
	"github.com/f5/otel-arrow-adapter/pkg/benchmark/dataset"
)

var help = flag.Bool("help", false, "Show help")

var inputFile = "data/otlp_traces.json"
var outputFile = "./data/nth_first_otlp_traces.json"
var spanCount = 10

// This tool extracts the first n spans from a compressed json file of traces (i.e. kind of `head` command for spans).
func main() {
	// Define the flags.
	flag.StringVar(&inputFile, "input", inputFile, "Input file")
	flag.StringVar(&outputFile, "output", outputFile, "Output file")
	flag.IntVar(&spanCount, "span_count", spanCount, "Number of spans")

	// Parse the flag
	flag.Parse()

	// Usage Demo
	if *help {
		flag.Usage()
		os.Exit(0)
	}

	// Extract the first n spans
	ds := dataset.NewRealTraceDataset(inputFile, benchmark.CompressionTypeZstd, []string{"trace_id"})
	if ds.SizeInBytes() == 0 {
		log.Fatal("failed to read any bytes from input")
	}
	fmt.Println(ds.SizeInBytes())
	traces := ds.Traces(0, spanCount)
	request := ptraceotlp.NewExportRequestFromTraces(traces[0])

	js, err := request.MarshalJSON()
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	// Write protobuf to file
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		err = os.MkdirAll(path.Dir(outputFile), 0700)
		if err != nil {
			log.Fatal("error creating directory: ", err)
		}
	}

	err = os.WriteFile(outputFile, js, 0600)
	if err != nil {
		log.Fatal("write error: ", err)
	}
}
