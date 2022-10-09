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

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"otel-arrow-adapter/pkg/datagen"
	"otel-arrow-adapter/pkg/otel/assert"
)

// TestOtlpToOtlpArrowConversion tests the conversion of OTLP traces to Arrow and back to OTLP.
//
// This test is based on the JSON serialization of the initial generated OTLP traces compared to the JSON serialization
// of the OTLP traces generated from the Arrow records.
func TestOtlpToOtlpArrowConversion(t *testing.T) {
	t.Parallel()

	tracesGen := datagen.NewTraceGenerator(datagen.DefaultResourceAttributes(), datagen.DefaultInstrumentationScopes())

	// Generate a random OTLP traces request.
	initialRequest := ptraceotlp.NewRequestFromTraces(tracesGen.Generate(10, 100))

	// Convert the OTLP traces request to Arrow.
	otlpArrowProducer := NewOtlpArrowProducer()
	records, err := otlpArrowProducer.ProduceFrom(initialRequest.Traces())
	if err != nil {
		t.Fatal(err)
	}

	// Convert the Arrow records back to OTLP.
	otlpProducer := NewOtlpProducer()
	for _, record := range records {
		traces, err := otlpProducer.ProduceFrom(record)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equiv(t, []ptrace.Traces{initialRequest.Traces()}, traces)
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
