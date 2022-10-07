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
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"otel-arrow-adapter/pkg/air"
	"otel-arrow-adapter/pkg/air/config"
	"otel-arrow-adapter/pkg/datagen"
)

func TestOtlpToOtlpArrowConversion(t *testing.T) {
	t.Parallel()

	cfg := config.NewUint16DefaultConfig()
	cfg.Attribute.Encoding = config.AttributesAsListStructs // TODO should become the default configuration.

	rr := air.NewRecordRepository(cfg)
	tracesGen := datagen.NewTraceGenerator(datagen.DefaultResourceAttributes(), datagen.DefaultInstrumentationScopes())

	// Generate a random OTLP traces request.
	initialRequest := ptraceotlp.NewRequestFromTraces(tracesGen.Generate(10, 100))

	expectedJson, err := initialRequest.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	// Convert the OTLP traces request to Arrow.
	records, err := OtlpTracesToArrowRecords(rr, initialRequest.Traces(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Convert the Arrow records back to OTLP.
	for _, record := range records {
		traces, err := ArrowRecordToOtlpTraces(record)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 1, len(traces), "there should be exactly one trace in the request")
		for _, trace := range traces {
			observedRequest := ptraceotlp.NewRequestFromTraces(trace)
			observedJson, err := observedRequest.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			assert.JSONEq(t, string(expectedJson), string(observedJson), "the observed request should be equal to the expected request")
		}
	}
}

func PrettyPrintRequest(request ptraceotlp.Request) error {
	jsonStr, err := request.MarshalJSON()
	if err != nil {
		return err
	}
	var prettyJSON bytes.Buffer
	err = json.Indent(&prettyJSON, jsonStr, "", "  ")
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", string(prettyJSON.Bytes()))
	return nil
}
