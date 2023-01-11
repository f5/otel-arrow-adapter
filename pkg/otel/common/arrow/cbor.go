/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package arrow

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/fxamacker/cbor/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

var (
	errInvalidKeyMap         = errors.New("invalid key map")
	errUnsupportedCborType   = errors.New("unsupported cbor type")
	errInvalidTypeConversion = errors.New("invalid type conversion")
)

// Serialize serializes the given pcommon.value into a CBOR byte array.
func Serialize(v pcommon.Value) ([]byte, error) {
	var buf bytes.Buffer

	em, err := cbor.EncOptions{Sort: cbor.SortCanonical}.EncMode()
	if err != nil {
		return nil, err
	}

	if err = encode(em.NewEncoder(&buf), v); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Deserialize deserializes the given CBOR byte array into a pcommon.value.
func Deserialize(cborData []byte) (pcommon.Value, error) {
	dec := cbor.NewDecoder(bytes.NewReader(cborData))

	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return pcommon.NewValueEmpty(), err
	}

	pv := pcommon.NewValueEmpty()
	if err := decode(v, &pv); err != nil {
		return pcommon.NewValueEmpty(), err
	}
	return pv, nil
}

func encode(enc *cbor.Encoder, v pcommon.Value) (err error) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		err = enc.Encode(v.Str())
	case pcommon.ValueTypeInt:
		err = enc.Encode(v.Int())
	case pcommon.ValueTypeDouble:
		err = enc.Encode(v.Double())
	case pcommon.ValueTypeBool:
		err = enc.Encode(v.Bool())
	case pcommon.ValueTypeMap:
		err = enc.StartIndefiniteMap()
		if err != nil {
			return
		}
		v.Map().Range(func(k string, v pcommon.Value) bool {
			if err := enc.Encode(k); err != nil {
				return false
			}
			if err := encode(enc, v); err != nil {
				return false
			}
			return true
		})
		err = enc.EndIndefinite()
		if err != nil {
			return
		}
	case pcommon.ValueTypeSlice:
		err = enc.StartIndefiniteArray()
		if err != nil {
			return
		}

		slice := v.Slice()
		for i := 0; i < slice.Len(); i++ {
			if err := encode(enc, slice.At(i)); err != nil {
				return err
			}
		}

		err = enc.EndIndefinite()
		if err != nil {
			return
		}
	case pcommon.ValueTypeBytes:
		err = enc.Encode(v.Bytes().AsRaw())
	case pcommon.ValueTypeEmpty:
		err = enc.Encode(nil)
	}
	return
}

func decode(inVal interface{}, outVal *pcommon.Value) error {
	switch typedV := inVal.(type) {
	case string:
		outVal.SetStr(typedV)
	case int:
		outVal.SetInt(int64(typedV))
	case int64:
		outVal.SetInt(typedV)
	case uint64:
		if typedV > math.MaxInt64 {
			return fmt.Errorf("uint64 too large to be converted into an int64: %w", errInvalidTypeConversion)
		}
		outVal.SetInt(int64(typedV))
	case float64:
		outVal.SetDouble(typedV)
	case bool:
		outVal.SetBool(typedV)
	case map[interface{}]interface{}:
		mapV := outVal.SetEmptyMap()

		for k, v := range typedV {
			if kStr, ok := k.(string); ok {
				emptyValue := mapV.PutEmpty(kStr)

				if err := decode(v, &emptyValue); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("key map is not a string: %w", errInvalidKeyMap)
			}
		}
	case []interface{}:
		slice := outVal.SetEmptySlice()
		slice.EnsureCapacity(len(typedV))
		for _, v := range typedV {
			elemV := slice.AppendEmpty()
			if err := decode(v, &elemV); err != nil {
				return err
			}
		}
	case []byte:
		binary := outVal.SetEmptyBytes()
		binary.Append(typedV...)
	case nil:
		// nothing to do
	default:
		return fmt.Errorf("unsupported CBOR type=%T: %w", inVal, errUnsupportedCborType)
	}
	return nil
}
