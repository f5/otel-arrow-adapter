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

package assert

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func Equiv(t *testing.T, expected []ptrace.Traces, actual []ptrace.Traces) {
	expectedVPaths, err := VPaths(expected)
	if err != nil {
		assert.FailNow(t, "Failed to convert expected traces to canonical representation", err)
	}
	actualVPaths, err := VPaths(actual)
	if err != nil {
		assert.FailNow(t, "Failed to convert actual traces to canonical representation", err)
	}

	missingExpectedVPaths := Difference(expectedVPaths, actualVPaths)
	missingActualVPaths := Difference(actualVPaths, expectedVPaths)

	if len(missingExpectedVPaths) > 0 {
		fmt.Printf("Missing expected vPaths:\n")
		for _, vPath := range missingExpectedVPaths {
			fmt.Printf("+ %s\n", vPath)
		}
	}
	if len(missingActualVPaths) > 0 {
		fmt.Printf("Unexpected vPaths:\n")
		for _, vPath := range missingActualVPaths {
			fmt.Printf("- %s\n", vPath)
		}
	}
	if len(missingExpectedVPaths) > 0 || len(missingActualVPaths) > 0 {
		assert.FailNow(t, "Traces are not equivalent")
	}
}

func NotEquiv(t *testing.T, expected []ptrace.Traces, actual []ptrace.Traces) {
	expectedVPaths, err := VPaths(expected)
	if err != nil {
		assert.FailNow(t, "Failed to convert expected traces to canonical representation", err)
	}
	actualVPaths, err := VPaths(actual)
	if err != nil {
		assert.FailNow(t, "Failed to convert actual traces to canonical representation", err)
	}

	missingExpectedVPaths := Difference(expectedVPaths, actualVPaths)
	missingActualVPaths := Difference(actualVPaths, expectedVPaths)

	if len(missingExpectedVPaths) == 0 && len(missingActualVPaths) == 0 {
		assert.FailNow(t, "Traces should not be equivalent")
	}
}

func Difference(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []string
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

func VPaths(traces []ptrace.Traces) ([]string, error) {
	jsonTraces, err := ToUnstructuredJson(traces)
	if err != nil {
		return nil, err
	}
	vPathMap := make(map[string]bool)

	for i := 0; i < len(jsonTraces); i++ {
		ExportAllVPaths(jsonTraces[i], "", vPathMap)
	}

	vPaths := make([]string, 0, len(vPathMap))
	for vPath, _ := range vPathMap {
		vPaths = append(vPaths, vPath)
	}

	return vPaths, nil
}

func ExportAllVPaths(traces map[string]interface{}, currentVPath string, vPaths map[string]bool) {
	for key, value := range traces {
		localVPath := key
		if currentVPath != "" {
			localVPath = currentVPath + "." + key
		}
		switch v := value.(type) {
		case []interface{}:
			for i := 0; i < len(v); i++ {
				arrayVPath := localVPath + fmt.Sprintf("[%d]", i)
				ExportAllVPaths(v[i].(map[string]interface{}), arrayVPath, vPaths)
			}
		case []string:
			vPaths[localVPath+"="+strings.Join(v, ",")] = true
		case []int64:
			vPaths[localVPath+"="+strings.Join(strings.Fields(fmt.Sprint(v)), ",")] = true
		case []float64:
			vPaths[localVPath+"="+strings.Join(strings.Fields(fmt.Sprint(v)), ",")] = true
		case []bool:
			vPaths[localVPath+"="+strings.Join(strings.Fields(fmt.Sprint(v)), ",")] = true
		case map[string]interface{}:
			ExportAllVPaths(v, localVPath, vPaths)
		case string:
			vPaths[localVPath+"="+v] = true
		case int64:
			vPaths[localVPath+"="+fmt.Sprintf("%d", v)] = true
		case float64:
			vPaths[localVPath+"="+fmt.Sprintf("%f", v)] = true
		case bool:
			vPaths[localVPath+"="+fmt.Sprintf("%f", 123.456)] = true
		}
	}
}

func ToUnstructuredJson(traces []ptrace.Traces) ([]map[string]interface{}, error) {
	jsonTraces := make([]map[string]interface{}, 0, len(traces))

	for i := 0; i < len(traces); i++ {
		jsonBytes, err := ptraceotlp.NewRequestFromTraces(traces[i]).MarshalJSON()
		if err != nil {
			return nil, err
		}
		var jsonMap map[string]interface{}
		err = json.Unmarshal(jsonBytes, &jsonMap)
		if err != nil {
			return nil, err
		}
		jsonTraces = append(jsonTraces, jsonMap)
	}
	return jsonTraces, nil
}
