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
	"crypto/rand"
	"flag"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"path"

	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/f5/otel-arrow-adapter/pkg/datagen"
)

var help = flag.Bool("help", false, "Show help")
var outputFile = "./data/otlp_traces.json"
var batchSize = 20

// This tool generates a trace dataset in the OpenTelemetry Protocol format from a fake traces generator.
func main() {
	// Define the flags.
	flag.StringVar(&outputFile, "output", outputFile, "Output file")
	flag.IntVar(&batchSize, "batchsize", batchSize, "Batch size")

	// Parse the flag
	flag.Parse()

	// Usage Demo
	if *help {
		flag.Usage()
		os.Exit(0)
	}

	// Generate the dataset.
	v, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		log.Fatalf("Failed to generate random number - %v", err)
	}
	entropy := datagen.NewTestEntropy(v.Int64())
	generator := datagen.NewTracesGenerator(entropy, entropy.NewStandardResourceAttributes(), entropy.NewStandardInstrumentationScopes())

	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		err = os.MkdirAll(path.Dir(outputFile), 0700)
		if err != nil {
			log.Fatal("error creating directory: ", err)
		}
	}
	f, err := os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatal("failed to open file: ", err)
	}

	fw, err := zstd.NewWriter(f)
	if err != nil {
		log.Fatal("error creating compressed writer", err)
	}
	defer fw.Close()

	for i := 0; i < batchSize; i++ {
		request := ptraceotlp.NewExportRequestFromTraces(generator.Generate(1, 100))

		// Marshal the request to bytes.
		msg, err := request.MarshalJSON()
		if err != nil {
			log.Fatal("marshaling error: ", err)
		}
		if _, err := fw.Write(msg); err != nil {
			log.Fatal("writing error: ", err)
		}
		if _, err := io.WriteString(fw, "\n"); err != nil {
			log.Fatal("writing newline error: ", err)
		}
	}

	fw.Flush()
}
