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

package metrics

import (
	"encoding/binary"
	"math"
	"sort"

	commonpb "go.opentelemetry.io/collector/pdata/pcommon"
	v1 "go.opentelemetry.io/collector/pdata/pmetric"
)

type KeyValues pcommon.Map

// Sort interface
func (kvs KeyValues) Less(i, j int) bool { return kvs[i].Key < kvs[j].Key }
func (kvs KeyValues) Len() int           { return len(kvs) }
func (kvs KeyValues) Swap(i, j int)      { kvs[i], kvs[j] = kvs[j], kvs[i] }

func DataPointSig(dataPoint *v1.NumberDataPoint, multivariateKey string) []byte {
	sig := make([]byte, 16, 128)

	// Serialize times and attributes to build the signature.
	binary.LittleEndian.PutUint64(sig[0:], dataPoint.StartTimeUnixNano)
	binary.LittleEndian.PutUint64(sig[8:], dataPoint.TimeUnixNano)
	KeyValuesSig(&sig, dataPoint.Attributes, multivariateKey)
	return sig
}

func KeyValuesSig(sig *[]byte, kvs pcommon.Map, multivariateKey string) {
	// Sort KeyValue slice by key to make the signature deterministic.
	sort.Sort(KeyValues(kvs))

	for _, kv := range kvs {
		// Skip the multivariate key.
		if kv.Key == multivariateKey {
			continue
		}

		// Serialize attribute name
		*sig = append(*sig, []byte(kv.Key)...)

		// Serialize attribute value
		ValueSig(sig, kv.Value)
	}
}

func ValueSig(sig *[]byte, value pcommon.Value) {
	switch value.Value.(type) {
	case pcommon.Value_BoolValue:
		*sig = append(*sig, BoolToByte(value.GetBoolValue()))
	case pcommon.Value_IntValue:
		if cap(*sig)-len(*sig) < 8 {
			*sig = append(make([]byte, 0, len(*sig)+8), *sig...)
		}
		pos := len(*sig)
		*sig = append(*sig, make([]byte, 8)...)
		binary.LittleEndian.PutUint64((*sig)[pos:], uint64(value.GetIntValue()))
	case pcommon.Value_DoubleValue:
		if cap(*sig)-len(*sig) < 8 {
			*sig = append(make([]byte, 0, len(*sig)+8), *sig...)
		}
		pos := len(*sig)
		*sig = append(*sig, make([]byte, 8)...)
		binary.LittleEndian.PutUint64((*sig)[pos:], math.Float64bits(value.GetDoubleValue()))
	case pcommon.Value_BytesValue:
		*sig = append(*sig, value.GetBytesValue()...)
	case pcommon.Value_StringValue:
		*sig = append(*sig, []byte(value.GetStringValue())...)
	case pcommon.Value_KvlistValue:
		KeyValuesSig(sig, value.GetKvlistValue().Values, "")
	case pcommon.Value_ArrayValue:
		for _, item := range value.GetArrayValue().Values {
			ValueSig(sig, item)
		}
	default:
		panic("unsupported value type")
	}
}

func BoolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
