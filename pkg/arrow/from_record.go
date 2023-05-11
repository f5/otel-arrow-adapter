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

// Utility functions to extract values from Arrow Records.

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// U16FromRecord returns the uint16 value for a specific row and column in an
// Arrow record. If the value is null, it returns 0.
func U16FromRecord(record arrow.Record, fieldID int, row int) (uint16, error) {
	if fieldID == -1 {
		return 0, nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return 0, nil
	}

	switch arr := arr.(type) {
	case *array.Uint16:
		if arr.IsNull(row) {
			return 0, nil
		} else {
			return arr.Value(row), nil
		}
	default:
		return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a uint16 array")
	}
}

// U32FromRecord returns the uint32 value for a specific row and column in an
// Arrow record. If the value is null, it returns 0.
func U32FromRecord(record arrow.Record, fieldID int, row int) (uint32, error) {
	if fieldID == -1 {
		return 0, nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return 0, nil
	}

	switch arr := arr.(type) {
	case *array.Uint32:
		if arr.IsNull(row) {
			return 0, nil
		} else {
			return arr.Value(row), nil
		}
	case *array.Dictionary:
		u32Arr := arr.Dictionary().(*array.Uint32)
		if arr.IsNull(row) {
			return 0, nil
		} else {
			return u32Arr.Value(arr.GetValueIndex(row)), nil
		}
	default:
		return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a uint32 array")
	}
}

// U64FromRecord returns the uint64 value for a specific row and column in an
// Arrow record. If the value is null, it returns 0.
func U64FromRecord(record arrow.Record, fieldID int, row int) (uint64, error) {
	if fieldID == -1 {
		return 0, nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return 0, nil
	}

	switch arr := arr.(type) {
	case *array.Uint64:
		if arr.IsNull(row) {
			return 0, nil
		} else {
			return arr.Value(row), nil
		}
	default:
		return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a uint32 array")
	}
}

// NullableU32FromRecord returns the uint32 value for a specific row and column in an
// Arrow record. If the value is null, it returns nil.
func NullableU32FromRecord(record arrow.Record, fieldID int, row int) (*uint32, error) {
	if fieldID == -1 {
		return nil, nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return nil, nil
	}

	if arr.IsNull(row) {
		return nil, nil
	}

	switch arr := arr.(type) {
	case *array.Uint32:
		if arr.IsNull(row) {
			return nil, nil
		} else {
			val := arr.Value(row)
			return &val, nil
		}
	default:
		return nil, werror.WrapWithMsg(ErrInvalidArrayType, "not a uint32 array")
	}
}

// I32FromRecord returns the int32 value for a specific row and column in an
// Arrow record. If the value is null, it returns 0.
func I32FromRecord(record arrow.Record, fieldID int, row int) (int32, error) {
	if fieldID == -1 {
		return 0, nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return 0, nil
	}

	switch arr := arr.(type) {
	case *array.Int32:
		if arr.IsNull(row) {
			return 0, nil
		} else {
			return arr.Value(row), nil
		}
	case *array.Dictionary:
		i32Arr := arr.Dictionary().(*array.Int32)
		if arr.IsNull(row) {
			return 0, nil
		} else {
			return i32Arr.Value(arr.GetValueIndex(row)), nil
		}
	default:
		return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a int32 array")
	}
}

// I64FromRecord returns the int64 value for a specific row and column in an
// Arrow record. If the value is null, it returns 0.
func I64FromRecord(record arrow.Record, fieldID int, row int) (int64, error) {
	if fieldID == -1 {
		return 0, nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return 0, nil
	}

	switch arr := arr.(type) {
	case *array.Int64:
		if arr.IsNull(row) {
			return 0, nil
		} else {
			return arr.Value(row), nil
		}
	case *array.Dictionary:
		i64Arr := arr.Dictionary().(*array.Int64)
		if arr.IsNull(row) {
			return 0, nil
		} else {
			return i64Arr.Value(arr.GetValueIndex(row)), nil
		}
	default:
		return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a int64 array")
	}
}

// F64FromRecord returns the float64 value for a specific row and column in an
// Arrow record. If the value is null, it returns 0.
func F64FromRecord(record arrow.Record, fieldID int, row int) (float64, error) {
	if fieldID == -1 {
		return 0, nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return 0, nil
	}

	switch arr := arr.(type) {
	case *array.Float64:
		if arr.IsNull(row) {
			return 0, nil
		} else {
			return arr.Value(row), nil
		}
	default:
		return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a float64 array")
	}
}

// F64OrNilFromRecord returns the float64 value for a specific row and column in an
// Arrow record. Returns nil if the value is null
func F64OrNilFromRecord(record arrow.Record, fieldID int, row int) (*float64, error) {
	if fieldID == -1 {
		return nil, nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return nil, nil
	}

	switch arr := arr.(type) {
	case *array.Float64:
		if arr.IsNull(row) {
			return nil, nil
		} else {
			v := arr.Value(row)
			return &v, nil
		}
	default:
		return nil, werror.WrapWithMsg(ErrInvalidArrayType, "not a float64 array")
	}
}

// BoolFromRecord returns the bool value for a specific row and column in an
// Arrow record. If the value is null, it returns false.
func BoolFromRecord(record arrow.Record, fieldID int, row int) (bool, error) {
	if fieldID == -1 {
		return false, nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return false, nil
	}

	switch arr := arr.(type) {
	case *array.Boolean:
		if arr.IsNull(row) {
			return false, nil
		} else {
			return arr.Value(row), nil
		}
	default:
		return false, werror.WrapWithMsg(ErrInvalidArrayType, "not a boolean array")
	}
}

// StringFromRecord returns the string value for a specific row and column in
// an Arrow record. If the value is null, it returns an empty string.
func StringFromRecord(record arrow.Record, fieldID int, row int) (string, error) {
	if fieldID == -1 {
		return "", nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return "", nil
	}

	return StringFromArray(arr, row)
}

// BinaryFromRecord returns the []byte value for a specific row and column in
// an Arrow record. If the value is null, it returns nil.
func BinaryFromRecord(record arrow.Record, fieldID int, row int) ([]byte, error) {
	if fieldID == -1 {
		return nil, nil
	}

	arr := record.Column(fieldID)
	if arr == nil {
		return nil, nil
	}

	return BinaryFromArray(arr, row)
}

// StructFromRecord returns the struct array for a specific row and
// column in an Arrow record. If the value is null, it returns nil.
func StructFromRecord(record arrow.Record, fieldID int, row int) (sarr *array.Struct, err error) {
	if fieldID == -1 {
		return nil, nil
	}

	column := record.Column(fieldID)
	switch arr := column.(type) {
	case *array.Struct:
		if arr.IsNull(row) {
			return
		}

		sarr = arr
	default:
		err = werror.WrapWithContext(common.ErrNotArrayMap, map[string]interface{}{"row": row, "fieldID": fieldID})
	}
	return
}

// SparseUnionFromRecord returns the sparse union array for a specific row and
// column in an Arrow record. If the value is null, it returns nil.
func SparseUnionFromRecord(record arrow.Record, fieldID int, row int) (marr *array.SparseUnion, err error) {
	if fieldID == -1 {
		return nil, nil
	}

	column := record.Column(fieldID)
	switch arr := column.(type) {
	case *array.SparseUnion:
		if arr.IsNull(row) {
			return
		}

		marr = arr
	default:
		err = werror.WrapWithContext(common.ErrNotArrayMap, map[string]interface{}{"row": row, "fieldID": fieldID})
	}
	return
}

// TimestampFromRecord returns the timestamp value for a specific row and column
// in an Arrow record. If the value is null, it returns 0.
func TimestampFromRecord(record arrow.Record, fieldID int, row int) (arrow.Timestamp, error) {
	if fieldID == -1 {
		return 0, nil
	}

	arr := record.Column(fieldID)

	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Timestamp:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0, werror.WrapWithMsg(ErrInvalidArrayType, "not a timestamp array")
		}
	}
}

// FixedSizeBinaryFieldByIDFromRecord returns the fixed size binary value of a field id for a specific row.
// If the value is null, it returns nil.
func FixedSizeBinaryFieldByIDFromRecord(record arrow.Record, fieldID int, row int) ([]byte, error) {
	if fieldID == -1 {
		return nil, nil
	}

	arr := record.Column(fieldID)

	if arr == nil {
		return nil, nil
	}

	return FixedSizeBinaryFromArray(arr, row)
}

// ListValuesByIDFromRecord return the list array for a field id for a specific row.
func ListValuesByIDFromRecord(record arrow.Record, fieldID int, row int) (arr arrow.Array, start int, end int, err error) {
	if fieldID == -1 {
		return nil, 0, 0, nil
	}

	column := record.Column(fieldID)
	switch listArr := column.(type) {
	case *array.List:
		if listArr.IsNull(row) {
			return nil, 0, 0, nil
		}
		start = int(listArr.Offsets()[row])
		end = int(listArr.Offsets()[row+1])
		arr = listArr.ListValues()
	default:
		err = werror.WrapWithContext(ErrNotArrayList, map[string]interface{}{"fieldID": fieldID, "row": row})
	}
	return
}
