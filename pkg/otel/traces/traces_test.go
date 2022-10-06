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

package traces

import (
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"otel-arrow-adapter/pkg/air"
	"otel-arrow-adapter/pkg/air/config"
	"otel-arrow-adapter/pkg/datagen"
)

// TODO: implement a series of tests to verify that the conversion from OTLP to Arrow and vice versa is correct.

func TestOtlpToOtlpArrowConversion(t *testing.T) {
	t.Parallel()

	cfg := config.NewUint16DefaultConfig()
	cfg.Attribute.Encoding = config.AttributesAsListStructs // TODO should become the default configuration.

	rr := air.NewRecordRepository(cfg)
	tracesGen := datagen.NewTraceGenerator(datagen.DefaultResourceAttributes(), datagen.DefaultInstrumentationScopes())
	request := ptraceotlp.NewRequestFromTraces(tracesGen.Generate(10, 100))

	records, err := OtlpTracesToArrowRecords(rr, request.Traces(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	for _, record := range records {
		fmt.Printf("#rows %d, #cols %d\n", record.NumRows(), record.NumCols())

		traces, err := ArrowRecordToOtlpTraces(record)
		if err != nil {
			t.Fatal(err)
		}

		spew.Dump(traces)
	}
}
