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
	"testing"

	"github.com/zeebo/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func TestEquiv(t *testing.T) {
	t.Parallel()

	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("foo1", "bar")
	rs.Resource().Attributes().PutInt("foo2", 123)
	rs.Resource().Attributes().PutDouble("foo3", 123.0)
	rs.Resource().Attributes().PutBool("foo4", true)
	rs.SetSchemaUrl("https://foo.bar")

	expectedTraces := []json.Marshaler{
		ptraceotlp.NewExportRequestFromTraces(traces),
	}

	actualTraces := []json.Marshaler{
		ptraceotlp.NewExportRequestFromTraces(traces),
		ptraceotlp.NewExportRequestFromTraces(traces),
	}
	Equiv(t, expectedTraces, actualTraces)

	traces = ptrace.NewTraces()
	rs = traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("foo", "bar")
	rs.Resource().Attributes().PutStr("baz", "qux")
	rs.SetSchemaUrl("https://foo.bar")
	actualTraces = []json.Marshaler{
		ptraceotlp.NewExportRequestFromTraces(traces),
	}
	NotEquiv(t, expectedTraces, actualTraces)
}

func TestTryAttributesSig(t *testing.T) {
	t.Parallel()

	// Valid case
	attrs := make([]interface{}, 0)
	attrs = append(attrs, attribute("key2", "value2"))
	attrs = append(attrs, attribute("key1", "value1"))
	attrs = append(attrs, attribute("key3", "value3"))
	attrsSig, done := tryAttributesSig(attrs)
	// attrs is a valid slice of attributes.
	assert.True(t, done)
	// All key/value pairs are sorted by key.
	assert.Equal(t, attrsSig, "{key1=value1,key2=value2,key3=value3}")

	// Empty attributes
	attrs = make([]interface{}, 0)
	attrsSig, done = tryAttributesSig(attrs)
	// attrs is a valid slice of attributes.
	assert.True(t, done)
	// All key/value pairs are sorted by key.
	assert.Equal(t, attrsSig, "{}")

	// Complex attributes
	attrs = make([]interface{}, 0)
	attrs = append(attrs, attribute("key2", "value2"))
	attrs = append(attrs, attribute("key1", "value1"))
	attrs = append(attrs, map[string]interface{}{
		"key": "key3",
		"value": map[string]interface{}{
			"service.name":    "my-service",
			"service.version": "1.0.0",
			"host.name":       "my-host",
		},
	})
	attrs = append(attrs, map[string]interface{}{
		"key":   "key0",
		"value": []interface{}{int64(1), "one", true, 1.23, false},
	})
	attrs = append(attrs, map[string]interface{}{
		"key":   "key4",
		"value": []bool{true, false, true},
	})
	attrsSig, done = tryAttributesSig(attrs)
	// attrs is a valid slice of attributes.
	assert.True(t, done)
	// All key/value pairs are sorted by key.
	assert.Equal(t, attrsSig, "{key0=[1,one,true,1.230000,false],key1=value1,key2=value2,key3={host.name=my-host,service.name=my-service,service.version=1.0.0},key4=[true,false,true]}")

	// Invalid case 1
	attrs = make([]interface{}, 0)
	attrs = append(attrs, attribute("key2", "value2"))
	attrs = append(attrs, attribute("key1", "value1"))
	attrs = append(attrs, map[string]interface{}{
		"value": 2,
	})
	_, done = tryAttributesSig(attrs)
	// attrs is not a valid slice of attributes.
	assert.False(t, done)

	// Invalid case 2
	attrs = make([]interface{}, 0)
	attrs = append(attrs, attribute("key2", "value2"))
	attrs = append(attrs, attribute("key1", "value1"))
	attrs = append(attrs, "bla")
	_, done = tryAttributesSig(attrs)
	// attrs is not a valid slice of attributes.
	assert.False(t, done)
}

func attribute(key string, value interface{}) interface{} {
	return map[string]interface{}{
		"key":   key,
		"value": value,
	}
}
