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

package arrow

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const BoolSig = "Bol"
const U8Sig = "U8"
const U16Sig = "U16"
const U32Sig = "U32"
const U64Sig = "U64"
const I8Sig = "I8"
const I16Sig = "I16"
const I32Sig = "I32"
const I64Sig = "I64"
const F32Sig = "F32"
const F64Sig = "F64"
const BinarySig = "Bin"
const StringSig = "Str"

type SortableField struct {
	name  *string
	field *arrow.Field
}

type Fields []SortableField

func (d Fields) Less(i, j int) bool {
	return *d[i].name < *d[j].name
}
func (d Fields) Len() int      { return len(d) }
func (d Fields) Swap(i, j int) { d[i], d[j] = d[j], d[i] }

func SchemaToID(schema *arrow.Schema) string {
	schemaID := ""
	fields := sortedFields(schema.Fields())
	for i := range fields {
		field := &fields[i]
		if i != 0 {
			schemaID += ","
		}
		schemaID += FieldToID(field.field)
	}
	return schemaID
}

func sortedFields(fields []arrow.Field) []SortableField {
	sortedField := make([]SortableField, len(fields))
	for i := 0; i < len(fields); i++ {
		sortedField[i] = SortableField{
			name:  &fields[i].Name,
			field: &fields[i],
		}
	}
	sort.Sort(Fields(sortedField))
	return sortedField
}

func FieldToID(field *arrow.Field) string {
	return field.Name + ":" + DataTypeToID(field.Type)
}

func DataTypeToID(dt arrow.DataType) string {
	id := ""
	switch t := dt.(type) {
	case *arrow.BooleanType:
		id += BoolSig
	case *arrow.Int8Type:
		id += I8Sig
	case *arrow.Int16Type:
		id += I16Sig
	case *arrow.Int32Type:
		id += I32Sig
	case *arrow.Int64Type:
		id += I64Sig
	case *arrow.Uint8Type:
		id += U8Sig
	case *arrow.Uint16Type:
		id += U16Sig
	case *arrow.Uint32Type:
		id += U32Sig
	case *arrow.Uint64Type:
		id += U64Sig
	case *arrow.Float32Type:
		id += F32Sig
	case *arrow.Float64Type:
		id += F64Sig
	case *arrow.StringType:
		id += StringSig
	case *arrow.BinaryType:
		id += BinarySig
	case *arrow.StructType:
		id += "{"
		fields := sortedFields(t.Fields())
		for i := range fields {
			if i > 0 {
				id += ","
			}
			id += FieldToID(fields[i].field)
		}
		id += "}"
	case *arrow.ListType:
		id += "["
		elemField := t.ElemField()
		id += DataTypeToID(elemField.Type)
		id += "]"
	case *arrow.DictionaryType:
		id += "Dic<"
		id += DataTypeToID(t.IndexType)
		id += ","
		id += DataTypeToID(t.ValueType)
		id += ">"
	case *arrow.DenseUnionType:
		// TODO implement
		id += "DenseUnion<>"
	case *arrow.SparseUnionType:
		// TODO implement
		id += "SparseUnion<>"
	case *arrow.MapType:
		// TODO implement
		id += "Map<>"
	case *arrow.FixedSizeBinaryType:
		// TODO implement
		id += "FixedSizeBinary<>"
	default:
		panic("unsupported data type " + dt.String())
	}

	return id
}

func ListOfStructsFieldIDFromSchema(schema *arrow.Schema, fieldName string) (int, *arrow.StructType, error) {
	ids := schema.FieldIndices(fieldName)
	if len(ids) == 0 {
		return 0, nil, fmt.Errorf("no field %q in schema", fieldName)
	}
	if len(ids) > 1 {
		return 0, nil, fmt.Errorf("more than one field %q in schema", fieldName)
	}
	if lt, ok := schema.Field(ids[0]).Type.(*arrow.ListType); ok {
		st, ok := lt.ElemField().Type.(*arrow.StructType)
		if !ok {
			return 0, nil, fmt.Errorf("field %q is not a list of structs", fieldName)
		}
		return ids[0], st, nil
	} else {
		return 0, nil, fmt.Errorf("field %q is not a list", fieldName)
	}
}

func ListOfStructsFieldIDFromStruct(dt *arrow.StructType, fieldName string) (int, *arrow.StructType, error) {
	id, ok := dt.FieldIdx(fieldName)
	if !ok {
		return 0, nil, fmt.Errorf("field %q not found", fieldName)
	}
	if lt, ok := dt.Field(id).Type.(*arrow.ListType); ok {
		st, ok := lt.ElemField().Type.(*arrow.StructType)
		if !ok {
			return 0, nil, fmt.Errorf("field %q is not a list of structs", fieldName)
		}
		return id, st, nil
	} else {
		return 0, nil, fmt.Errorf("field %q is not a list", fieldName)
	}
}

func StructFieldIDFromStruct(dt *arrow.StructType, fieldName string) (int, *arrow.StructType, error) {
	id, found := dt.FieldIdx(fieldName)
	if !found {
		return 0, nil, fmt.Errorf("no field %q in struct type", fieldName)
	}
	if st, ok := dt.Field(id).Type.(*arrow.StructType); ok {
		return id, st, nil
	} else {
		return 0, nil, fmt.Errorf("field %q is not a struct", fieldName)
	}
}

func FieldIDFromStruct(dt *arrow.StructType, fieldName string) (int, *arrow.DataType, error) {
	id, found := dt.FieldIdx(fieldName)
	if !found {
		return 0, nil, fmt.Errorf("no field %q in struct type", fieldName)
	}
	field := dt.Field(id)
	return id, &field.Type, nil
}

func OptionalFieldIDFromStruct(dt *arrow.StructType, fieldName string) (id int) {
	id, found := dt.FieldIdx(fieldName)
	if !found {
		id = -1
	}
	return
}

type ListOfStructs struct {
	dt    *arrow.StructType
	arr   *array.Struct
	start int
	end   int
}

// ListOfStructsFromRecord returns the struct type and an array of structs for a given field id.
func ListOfStructsFromRecord(record arrow.Record, fieldID int, row int) (*ListOfStructs, error) {
	arr := record.Column(fieldID)
	switch listArr := arr.(type) {
	case *array.List:
		if listArr.IsNull(row) {
			return nil, nil
		}
		switch structArr := listArr.ListValues().(type) {
		case *array.Struct:
			dt, ok := structArr.DataType().(*arrow.StructType)
			if !ok {
				return nil, fmt.Errorf("field id %d is not a list of structs", fieldID)
			}
			start := int(listArr.Offsets()[row])
			end := int(listArr.Offsets()[row+1])

			return &ListOfStructs{
				dt:    dt,
				arr:   structArr,
				start: start,
				end:   end,
			}, nil
		default:
			return nil, fmt.Errorf("field id %d is not a list of structs", fieldID)
		}
	default:
		return nil, fmt.Errorf("field id %d is not a list", fieldID)
	}
}

func ListOfStructsFromStruct(parent *array.Struct, fieldID int, row int) (*ListOfStructs, error) {
	arr := parent.Field(fieldID)
	if listArr, ok := arr.(*array.List); ok {
		if listArr.IsNull(row) {
			return nil, nil
		}
		switch structArr := listArr.ListValues().(type) {
		case *array.Struct:
			dt, ok := structArr.DataType().(*arrow.StructType)
			if !ok {
				return nil, fmt.Errorf("field id %d is not a list of structs", fieldID)
			}
			start := int(listArr.Offsets()[row])
			end := int(listArr.Offsets()[row+1])

			return &ListOfStructs{
				dt:    dt,
				arr:   structArr,
				start: start,
				end:   end,
			}, nil
		default:
			return nil, fmt.Errorf("field id %d is not a list of structs", fieldID)
		}
	} else {
		return nil, fmt.Errorf("field id %d is not a list", fieldID)
	}
}

func (los *ListOfStructs) Start() int {
	return los.start
}

func (los *ListOfStructs) End() int {
	return los.end
}

func (los *ListOfStructs) FieldIdx(name string) (int, bool) {
	return los.dt.FieldIdx(name)
}

func (los *ListOfStructs) Field(name string) (arrow.Array, bool) {
	id, ok := los.dt.FieldIdx(name)
	if !ok {
		return nil, false
	}
	return los.arr.Field(id), true
}

func (los *ListOfStructs) FieldByID(id int) arrow.Array {
	return los.arr.Field(id)
}

func (los *ListOfStructs) StringFieldByID(fieldID int, row int) (string, error) {
	column := los.arr.Field(fieldID)
	return StringFromArray(column, row)
}

func (los *ListOfStructs) U32FieldByID(fieldID int, row int) (uint32, error) {
	column := los.arr.Field(fieldID)
	return U32FromArray(column, row)
}

func (los *ListOfStructs) U64FieldByID(fieldID int, row int) (uint64, error) {
	column := los.arr.Field(fieldID)
	return U64FromArray(column, row)
}

func (los *ListOfStructs) OptionalTimestampFieldByID(fieldID int, row int) *pcommon.Timestamp {
	column := los.arr.Field(fieldID)
	if column.IsNull(row) {
		return nil
	}
	ts, err := U64FromArray(column, row)
	if err != nil {
		return nil
	}
	timestamp := pcommon.Timestamp(ts)
	return &timestamp
}

func (los *ListOfStructs) I32FieldByID(fieldID int, row int) (int32, error) {
	column := los.arr.Field(fieldID)
	return I32FromArray(column, row)
}

func (los *ListOfStructs) I64FieldByID(fieldID int, row int) (int64, error) {
	column := los.arr.Field(fieldID)
	return I64FromArray(column, row)
}

func (los *ListOfStructs) F64FieldByID(fieldID int, row int) (float64, error) {
	column := los.arr.Field(fieldID)
	return F64FromArray(column, row)
}

func (los *ListOfStructs) F64OrNilFieldByID(fieldID int, row int) (*float64, error) {
	column := los.arr.Field(fieldID)
	return F64OrNilFromArray(column, row)
}

func (los *ListOfStructs) BoolFieldByID(fieldID int, row int) (bool, error) {
	column := los.arr.Field(fieldID)
	return BoolFromArray(column, row)
}

func (los *ListOfStructs) BinaryFieldByID(fieldID int, row int) ([]byte, error) {
	column := los.arr.Field(fieldID)
	return BinaryFromArray(column, row)
}

func (los *ListOfStructs) FixedSizeBinaryFieldByID(fieldID int, row int) ([]byte, error) {
	column := los.arr.Field(fieldID)
	return FixedSizeBinaryFromArray(column, row)
}

func (los *ListOfStructs) StringFieldByName(name string, row int) (string, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return "", nil
	}
	column := los.arr.Field(fieldID)
	return StringFromArray(column, row)
}

func (los *ListOfStructs) U32FieldByName(name string, row int) (uint32, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return U32FromArray(column, row)
}

func (los *ListOfStructs) U64FieldByName(name string, row int) (uint64, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return U64FromArray(column, row)
}

func (los *ListOfStructs) I32FieldByName(name string, row int) (int32, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return I32FromArray(column, row)
}

func (los *ListOfStructs) I64FieldByName(name string, row int) (int64, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return 0, nil
	}
	column := los.arr.Field(fieldID)
	return I64FromArray(column, row)
}

func (los *ListOfStructs) F64FieldByName(name string, row int) (float64, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return 0.0, nil
	}
	column := los.arr.Field(fieldID)
	return F64FromArray(column, row)
}

func (los *ListOfStructs) BoolFieldByName(name string, row int) (bool, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return false, nil
	}
	column := los.arr.Field(fieldID)
	return BoolFromArray(column, row)
}

func (los *ListOfStructs) BinaryFieldByName(name string, row int) ([]byte, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return nil, nil
	}
	column := los.arr.Field(fieldID)
	return BinaryFromArray(column, row)
}

func (los *ListOfStructs) FixedSizeBinaryFieldByName(name string, row int) ([]byte, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return nil, nil
	}
	column := los.arr.Field(fieldID)
	return FixedSizeBinaryFromArray(column, row)
}

func (los *ListOfStructs) StructArray(name string, row int) (*arrow.StructType, *array.Struct, error) {
	fieldID, found := los.dt.FieldIdx(name)
	if !found {
		return nil, nil, nil
	}
	column := los.arr.Field(fieldID)
	switch structArr := column.(type) {
	case *array.Struct:
		if structArr.IsNull(row) {
			return nil, nil, nil
		}
		return structArr.DataType().(*arrow.StructType), structArr, nil
	default:
		return nil, nil, fmt.Errorf("field %q is not a struct", name)
	}
}

func (los *ListOfStructs) StructByID(fieldID int, row int) (*arrow.StructType, *array.Struct, error) {
	column := los.arr.Field(fieldID)
	switch structArr := column.(type) {
	case *array.Struct:
		if structArr.IsNull(row) {
			return nil, nil, nil
		}
		return structArr.DataType().(*arrow.StructType), structArr, nil
	default:
		return nil, nil, fmt.Errorf("field id %d is not a struct", fieldID)
	}
}

func (los *ListOfStructs) IsNull(row int) bool {
	return los.arr.IsNull(row)
}

func (los *ListOfStructs) ListValuesById(row int, fieldID int) (arr arrow.Array, start int, end int, err error) {
	column := los.arr.Field(fieldID)
	switch listArr := column.(type) {
	case *array.List:
		if listArr.IsNull(row) {
			return nil, 0, 0, nil
		}
		start = int(listArr.Offsets()[row])
		end = int(listArr.Offsets()[row+1])
		arr = listArr.ListValues()
	default:
		err = fmt.Errorf("field id %d is not a list", fieldID)
	}
	return
}

func (los *ListOfStructs) ListOfStructsById(row int, fieldID int) (*ListOfStructs, error) {
	column := los.arr.Field(fieldID)
	switch listArr := column.(type) {
	case *array.List:
		if listArr.IsNull(row) {
			return nil, nil
		}
		switch structArr := listArr.ListValues().(type) {
		case *array.Struct:
			dt, ok := structArr.DataType().(*arrow.StructType)
			if !ok {
				return nil, fmt.Errorf("field id %d is not a list of struct", fieldID)
			}
			start := int(listArr.Offsets()[row])
			end := int(listArr.Offsets()[row+1])

			return &ListOfStructs{
				dt:    dt,
				arr:   structArr,
				start: start,
				end:   end,
			}, nil
		default:
			return nil, fmt.Errorf("field id %d is not a list of structs", fieldID)
		}
	default:
		return nil, fmt.Errorf("field id %d is not a list", fieldID)
	}
}

func (los *ListOfStructs) DataType() *arrow.StructType {
	return los.dt
}

func (los *ListOfStructs) Array() *array.Struct {
	return los.arr
}

func BoolFromArray(arr arrow.Array, row int) (bool, error) {
	if arr == nil {
		return false, nil
	} else {
		switch arr := arr.(type) {
		case *array.Boolean:
			if arr.IsNull(row) {
				return false, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return false, fmt.Errorf("column is not of type bool")
		}
	}
}

func F64FromArray(arr arrow.Array, row int) (float64, error) {
	if arr == nil {
		return 0.0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Float64:
			if arr.IsNull(row) {
				return 0.0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0.0, fmt.Errorf("column is not of type f64")
		}
	}
}

func F64OrNilFromArray(arr arrow.Array, row int) (*float64, error) {
	if arr == nil {
		return nil, nil
	} else {
		switch arr := arr.(type) {
		case *array.Float64:
			if arr.IsNull(row) {
				return nil, nil
			} else {
				v := arr.Value(row)
				return &v, nil
			}
		default:
			return nil, fmt.Errorf("column is not of type f64")
		}
	}
}

func U64FromArray(arr arrow.Array, row int) (uint64, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Uint64:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0, fmt.Errorf("column is not of type uint64")
		}
	}
}

func U32FromArray(arr arrow.Array, row int) (uint32, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Uint32:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0, fmt.Errorf("column is not of type uint32")
		}
	}
}

func U32FromStruct(structArr *array.Struct, row int, fieldID int) (uint32, error) {
	return U32FromArray(structArr.Field(fieldID), row)
}

func I32FromArray(arr arrow.Array, row int) (int32, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Int32:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0, fmt.Errorf("column is not of type int32")
		}
	}
}

func I64FromArray(arr arrow.Array, row int) (int64, error) {
	if arr == nil {
		return 0, nil
	} else {
		switch arr := arr.(type) {
		case *array.Int64:
			if arr.IsNull(row) {
				return 0, nil
			} else {
				return arr.Value(row), nil
			}
		default:
			return 0, fmt.Errorf("column is not of type int64")
		}
	}
}

func StringFromArray(arr arrow.Array, row int) (string, error) {
	if arr == nil {
		return "", nil
	} else {
		if arr.IsNull(row) {
			return "", nil
		}

		switch arr := arr.(type) {
		case *array.String:
			return arr.Value(row), nil
		case *array.Dictionary:
			return arr.Dictionary().(*array.String).Value(arr.GetValueIndex(row)), nil
		default:
			return "", fmt.Errorf("column is not of type string")
		}
	}
}

func StringFromStruct(arr arrow.Array, row int, id int) (string, error) {
	structArr, ok := arr.(*array.Struct)
	if !ok {
		return "", fmt.Errorf("array id %d is not of type struct", id)
	}
	if structArr != nil {
		return StringFromArray(structArr.Field(id), row)
	} else {
		return "", fmt.Errorf("column array is not of type struct")
	}
}

func I32FromStruct(arr arrow.Array, row int, id int) (int32, error) {
	structArr, ok := arr.(*array.Struct)
	if !ok {
		return 0, fmt.Errorf("array id %d is not of type struct", id)
	}
	if structArr != nil {
		return I32FromArray(structArr.Field(id), row)
	} else {
		return 0, fmt.Errorf("column array is not of type struct")
	}
}

func BinaryFromArray(arr arrow.Array, row int) ([]byte, error) {
	if arr == nil {
		return nil, nil
	} else {
		if arr.IsNull(row) {
			return nil, nil
		}

		switch arr := arr.(type) {
		case *array.Binary:
			return arr.Value(row), nil
		case *array.Dictionary:
			return arr.Dictionary().(*array.Binary).Value(arr.GetValueIndex(row)), nil
		default:
			return nil, fmt.Errorf("column is not of type binary")
		}
	}
}

func FixedSizeBinaryFromArray(arr arrow.Array, row int) ([]byte, error) {
	if arr == nil {
		return nil, nil
	} else {
		if arr.IsNull(row) {
			return nil, nil
		}

		switch arr := arr.(type) {
		case *array.FixedSizeBinary:
			return arr.Value(row), nil
		case *array.Dictionary:
			return arr.Dictionary().(*array.FixedSizeBinary).Value(arr.GetValueIndex(row)), nil
		default:
			return nil, fmt.Errorf("column is not of type binary")
		}
	}
}

// DumpRecordInfo is a utility function to display the structure and few other attributes of an
// Apache Record.
// This function is test/debug purpose only.
func DumpRecordInfo(record arrow.Record) {
	schema := record.Schema()
	columnCount := 0
	emptyColumnCount := 0
	for i := 0; i < int(record.NumCols()); i++ {
		field := schema.Field(i)
		arr := record.Column(i)
		fmt.Printf("Field %q type: %q, null-count: %d, length: %d\n", field.Name, field.Type.Name(), arr.NullN(), arr.Len())
		columnCount++
		if arr.Len() == arr.NullN() {
			emptyColumnCount++
		}
		cc, scc := DumArrayInfo("\t", &field, arr)
		columnCount += cc
		emptyColumnCount += scc
	}
	fmt.Printf("#columns: %d\n", columnCount)
	fmt.Printf("# empty columns: %d\n", emptyColumnCount)
}

// DumArrayInfo is a utility function to display the structure and few other attributes of an
// // Apache Array.
// // This function is test/debug purpose only.
func DumArrayInfo(indent string, field *arrow.Field, arr arrow.Array) (int, int) {
	columnCount := 1
	emptyColumnCount := 0
	if arr.Len() == arr.NullN() {
		emptyColumnCount++
	}
	switch f := field.Type.(type) {
	case *arrow.StructType:
		arr := arr.(*array.Struct)
		fmt.Printf("%sField %q type: %q, null-count: %d, length: %d\n", indent, field.Name, field.Type.Name(), arr.NullN(), arr.Len())
		for i := 0; i < arr.NumField(); i++ {
			subField := f.Field(i)
			cc, ecc := DumArrayInfo(indent+"\t", &subField, arr.Field(i))
			columnCount += cc
			emptyColumnCount += ecc
		}
	case *arrow.ListType:
		arr := arr.(*array.List)
		fmt.Printf("%sField %q type: %q, null-count: %d, length: %d\n", indent, field.Name, field.Type.Name(), arr.NullN(), arr.Len())
		subField := f.ElemField()
		cc, ecc := DumArrayInfo(indent+"\t", &subField, arr.ListValues())
		columnCount += cc
		emptyColumnCount += ecc
	case *arrow.MapType:
		arr := arr.(*array.Map)
		fmt.Printf("%sField %q type: %q, null-count: %d, length: %d\n", indent, field.Name, field.Type.Name(), arr.NullN(), arr.Len())
		subField := f.KeyField()
		cc, ecc := DumArrayInfo(indent+"\t", &subField, arr.Keys())
		columnCount += cc
		emptyColumnCount += ecc
		subField = f.ValueField()
		cc, ecc = DumArrayInfo(indent+"\t", &subField, arr.ListValues())
		columnCount += cc
		emptyColumnCount += ecc
	case *arrow.SparseUnionType:
		arr := arr.(*array.SparseUnion)
		fmt.Printf("%sField %q type: %q, null-count: %d, length: %d\n", indent, field.Name, field.Type.Name(), arr.NullN(), arr.Len())
		for i, variant := range f.Fields() {
			cc, ecc := DumArrayInfo(indent+"\t", &variant, arr.Field(i))
			columnCount += cc
			emptyColumnCount += ecc
		}
	case *arrow.DenseUnionType:
		arr := arr.(*array.DenseUnion)
		fmt.Printf("%sField %q type: %q, null-count: %d, length: %d\n", indent, field.Name, field.Type.Name(), arr.NullN(), arr.Len())
		for i, variant := range f.Fields() {
			cc, ecc := DumArrayInfo(indent+"\t", &variant, arr.Field(i))
			columnCount += cc
			emptyColumnCount += ecc
		}
	default:
		fmt.Printf("%sField %q type: %q, null-count: %d, length: %d\n", indent, field.Name, field.Type.Name(), arr.NullN(), arr.Len())
	}
	return columnCount, emptyColumnCount
}
