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

package arrow_test

import (
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/stretchr/testify/assert"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
)

const (
	Root              = "root"
	U8                = "u8"
	U32               = "u32"
	U64               = "u64"
	Values            = "values"
	I64               = "i64"
	I32               = "i32"
	F64               = "f64"
	Bool              = "bool"
	Binary            = "binary"
	String            = "string"
	Map               = "map"
	Timestamp         = "timestamp"
	FixedSize8Binary  = "fixed_size_8_binary"
	FixedSize16Binary = "fixed_size_16_binary"
)

const (
	I64Code    int8 = 0
	F64Code    int8 = 1
	BoolCode   int8 = 2
	BinaryCode int8 = 3
	StringCode int8 = 4
)

var (
	valueDT = arrow.SparseUnionOf(
		[]arrow.Field{
			{Name: I64, Type: arrow.PrimitiveTypes.Int64, Metadata: acommon.OptionalField},
			{Name: F64, Type: arrow.PrimitiveTypes.Float64, Metadata: acommon.OptionalField},
			{Name: Bool, Type: arrow.FixedWidthTypes.Boolean, Metadata: acommon.OptionalField},
			{Name: Binary, Type: arrow.BinaryTypes.Binary, Metadata: acommon.OptionalField},
			{Name: String, Type: arrow.BinaryTypes.String, Metadata: acommon.OptionalField},
		},
		[]arrow.UnionTypeCode{
			I64Code,
			F64Code,
			BoolCode,
			BinaryCode,
			StringCode,
		})

	protoSchema = arrow.NewSchema([]arrow.Field{
		{Name: Root, Type: arrow.StructOf([]arrow.Field{
			{Name: Timestamp, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: acommon.OptionalField},
			{Name: U8, Type: arrow.PrimitiveTypes.Uint8, Metadata: acommon.OptionalField},
			{Name: U64, Type: arrow.PrimitiveTypes.Uint64, Metadata: acommon.OptionalField},
			{Name: I64, Type: arrow.PrimitiveTypes.Int64, Metadata: acommon.OptionalField},
			{Name: Bool, Type: arrow.FixedWidthTypes.Boolean, Metadata: acommon.OptionalField},
			{Name: Binary, Type: arrow.BinaryTypes.Binary, Metadata: acommon.OptionalField},
			{Name: U32, Type: arrow.PrimitiveTypes.Uint32, Metadata: acommon.OptionalField},
			{Name: I32, Type: arrow.PrimitiveTypes.Int32, Metadata: acommon.OptionalField},
			{Name: String, Type: arrow.BinaryTypes.String, Metadata: acommon.OptionalField},
			{Name: Values, Type: arrow.ListOf(valueDT), Metadata: acommon.OptionalField},
			{Name: FixedSize8Binary, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Metadata: acommon.OptionalField},
			{Name: FixedSize16Binary, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: acommon.OptionalField},
			{Name: Map, Type: arrow.MapOf(arrow.BinaryTypes.String, valueDT), Metadata: acommon.OptionalField},
		}...)},
	}, nil)
)

func TestTimestampOnly(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		timestamp: arrow.Timestamp(10),
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"timestamp\":\"1970-01-01 00:00:00.00000001\"}}\n]")
}

func TestU8Only(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		u8: 1,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"u8\":1}}\n]")
}

func TestU64Only(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		u64: 2,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"u64\":2}}\n]")
}

func TestI64Only(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		i64: 3,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"i64\":3}}\n]")
}

func TestBoolOnly(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		bool: true,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"bool\":true}}\n]")
}

func TestBinaryOnly(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		binary: []byte("binary"),
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"binary\":\"YmluYXJ5\"}}\n]")
}

func TestU32Only(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		u32: 4,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"u32\":4}}\n]")
}

func TestI32Only(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		i32: 5,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"i32\":5}}\n]")
}

func TestStringOnly(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		string: "string",
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"string\":\"string\"}}\n]")
}

func TestValuesOnly1(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		values: []ValueData{
			I64ValueData{1},
		},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"values\":[[0,1]]}}\n]")
}

func TestValuesOnly2(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		values: []ValueData{
			F64ValueData{2},
		},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"values\":[[1,2]]}}\n]")
}

func TestValuesOnly3(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		values: []ValueData{
			I64ValueData{1},
			F64ValueData{2},
		},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"values\":[[0,1],[1,2]]}}\n]")
}

func TestFixedSize8Only(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		fixedSize8: []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"fixed_size_8_binary\":\"AQIDBAUGBwg=\"}}\n]")
}

func TestFixedSize16Only(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		fixedSize16: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"fixed_size_16_binary\":\"AQIDBAUGBwgJCgsMDQ4PEA==\"}}\n]")
}

func TestHMapOnly1(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		hmap: map[string]ValueData{
			"key1": I64ValueData{1},
		},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"map\":[{\"key\":\"key1\",\"value\":[0,1]}]}}\n]")
}

func TestHMapOnly2(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)

	rootData := RootData{
		hmap: map[string]ValueData{
			"key2": F64ValueData{2},
		},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"map\":[{\"key\":\"key2\",\"value\":[1,2]}]}}\n]")
}

func TestSchemaEvolution(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	recordBuilderExt := builder.NewRecordBuilderExt(pool, protoSchema)
	defer recordBuilderExt.Release()

	rootBuilder := NewRootBuilderFrom(recordBuilderExt)
	rootData := RootData{
		timestamp: arrow.Timestamp(10),
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"timestamp\":\"1970-01-01 00:00:00.00000001\"}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u8\":1}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u8\":1,\"u64\":2}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u8\":1,\"u64\":2,\"i64\":3}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
		bool:      true,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u8\":1,\"u64\":2,\"i64\":3,\"bool\":true}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
		bool:      true,
		binary:    []byte("binary"),
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u8\":1,\"u64\":2,\"i64\":3,\"bool\":true,\"binary\":\"YmluYXJ5\"}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
		bool:      true,
		binary:    []byte("binary"),
		u32:       4,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u8\":1,\"u64\":2,\"i64\":3,\"bool\":true,\"binary\":\"YmluYXJ5\",\"u32\":4}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
		bool:      true,
		binary:    []byte("binary"),
		u32:       4,
		i32:       5,
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u8\":1,\"u64\":2,\"i64\":3,\"bool\":true,\"binary\":\"YmluYXJ5\",\"u32\":4,\"i32\":5}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
		bool:      true,
		binary:    []byte("binary"),
		u32:       4,
		i32:       5,
		string:    "string",
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u8\":1,\"u64\":2,\"i64\":3,\"bool\":true,\"binary\":\"YmluYXJ5\",\"u32\":4,\"i32\":5,\"string\":\"string\"}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
		bool:      true,
		binary:    []byte("binary"),
		u32:       4,
		i32:       5,
		string:    "string",
		values: []ValueData{
			I64ValueData{1},
			F64ValueData{2.0},
		},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"binary\":\"YmluYXJ5\",\"bool\":true,\"i32\":5,\"i64\":3,\"string\":\"string\",\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u32\":4,\"u64\":2,\"u8\":1,\"values\":[[0,1],[1,2]]}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
		bool:      true,
		binary:    []byte("binary"),
		u32:       4,
		i32:       5,
		string:    "string",
		values: []ValueData{
			I64ValueData{1},
			F64ValueData{2.0},
		},
		fixedSize8: []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"binary\":\"YmluYXJ5\",\"bool\":true,\"fixed_size_8_binary\":\"AQIDBAUGBwg=\",\"i32\":5,\"i64\":3,\"string\":\"string\",\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u32\":4,\"u64\":2,\"u8\":1,\"values\":[[0,1],[1,2]]}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
		bool:      true,
		binary:    []byte("binary"),
		u32:       4,
		i32:       5,
		string:    "string",
		values: []ValueData{
			I64ValueData{1},
			F64ValueData{2.0},
		},
		fixedSize8:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
		fixedSize16: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"binary\":\"YmluYXJ5\",\"bool\":true,\"fixed_size_16_binary\":\"AQIDBAUGBwgJCgsMDQ4PEA==\",\"fixed_size_8_binary\":\"AQIDBAUGBwg=\",\"i32\":5,\"i64\":3,\"string\":\"string\",\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u32\":4,\"u64\":2,\"u8\":1,\"values\":[[0,1],[1,2]]}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
		bool:      true,
		binary:    []byte("binary"),
		u32:       4,
		i32:       5,
		string:    "string",
		values: []ValueData{
			I64ValueData{1},
			F64ValueData{2.0},
		},
		fixedSize8:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
		fixedSize16: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		hmap: map[string]ValueData{
			"key1": I64ValueData{1},
		},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"binary\":\"YmluYXJ5\",\"bool\":true,\"fixed_size_16_binary\":\"AQIDBAUGBwgJCgsMDQ4PEA==\",\"fixed_size_8_binary\":\"AQIDBAUGBwg=\",\"i32\":5,\"i64\":3,\"map\":[{\"key\":\"key1\",\"value\":[0,1]}],\"string\":\"string\",\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u32\":4,\"u64\":2,\"u8\":1,\"values\":[[0,1],[1,2]]}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        1,
		u64:       2,
		i64:       3,
		bool:      true,
		binary:    []byte("binary"),
		u32:       4,
		i32:       5,
		string:    "string",
		values: []ValueData{
			I64ValueData{1},
			F64ValueData{2.0},
		},
		fixedSize8:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
		fixedSize16: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		hmap: map[string]ValueData{
			"key2": F64ValueData{2.0},
		},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"binary\":\"YmluYXJ5\",\"bool\":true,\"fixed_size_16_binary\":\"AQIDBAUGBwgJCgsMDQ4PEA==\",\"fixed_size_8_binary\":\"AQIDBAUGBwg=\",\"i32\":5,\"i64\":3,\"map\":[{\"key\":\"key2\",\"value\":[1,2]}],\"string\":\"string\",\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u32\":4,\"u64\":2,\"u8\":1,\"values\":[[0,1],[1,2]]}}\n]")

	rootData = RootData{
		timestamp: arrow.Timestamp(10),
		u8:        2,
		u64:       3,
		i64:       0,
		bool:      false,
		binary:    []byte("binary"),
		u32:       0,
		i32:       6,
		string:    "",
		values: []ValueData{
			F64ValueData{2.0},
		},
	}
	AddAndCheck(t, &rootData, rootBuilder, "[{\"root\":{\"binary\":\"YmluYXJ5\",\"bool\":false,\"fixed_size_16_binary\":null,\"fixed_size_8_binary\":null,\"i32\":6,\"i64\":0,\"map\":null,\"string\":null,\"timestamp\":\"1970-01-01 00:00:00.00000001\",\"u32\":null,\"u64\":3,\"u8\":2,\"values\":[[1,2]]}}\n]")
}

func AddAndCheck(t *testing.T, data *RootData, rootBuilder *RootBuilder, expectedJson string) {
	record := rootBuilder.AppendData(data)
	defer record.Release()
	json, err := record.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	assert.JSONEq(t, expectedJson, string(json))
}

type RootData struct {
	timestamp   arrow.Timestamp
	u8          uint8
	u64         uint64
	i64         int64
	bool        bool
	binary      []byte
	u32         uint32
	i32         int32
	string      string
	values      []ValueData
	fixedSize8  []byte
	fixedSize16 []byte
	hmap        map[string]ValueData
}

type ValueData interface {
	IsI64() bool
	IsF64() bool
	I64() int64
	F64() float64
}

type I64ValueData struct {
	i64 int64
}

func (v I64ValueData) IsI64() bool {
	return true
}

func (v I64ValueData) IsF64() bool {
	return false
}

func (v I64ValueData) I64() int64 {
	return v.i64
}

func (v I64ValueData) F64() float64 {
	panic("not implemented")
}

type F64ValueData struct {
	f64 float64
}

func (v F64ValueData) IsI64() bool {
	return false
}

func (v F64ValueData) IsF64() bool {
	return true
}

func (v F64ValueData) I64() int64 {
	panic("not implemented")
}

func (v F64ValueData) F64() float64 {
	return v.f64
}

type RootBuilder struct {
	recordBuilder *builder.RecordBuilderExt
	builder       *builder.StructBuilder
	timestamp     *builder.TimestampBuilder
	u8            *builder.Uint8Builder
	u64           *builder.Uint64Builder
	i64           *builder.Int64Builder
	bool          *builder.BooleanBuilder
	binary        *builder.BinaryBuilder
	u32           *builder.Uint32Builder
	i32           *builder.Int32Builder
	string        *builder.StringBuilder
	values        *ValuesBuilder
	fixedSize8    *builder.FixedSizeBinaryBuilder
	fixedSize16   *builder.FixedSizeBinaryBuilder
	hmap          *HMapBuilder
}

type ValuesBuilder struct {
	builder *builder.ListBuilder
	values  *ValueBuilder
}

type HMapBuilder struct {
	builder *builder.MapBuilder
	keys    *builder.StringBuilder
	values  *builder.SparseUnionBuilder
	i64     *builder.Int64Builder
	f64     *builder.Float64Builder
	bool    *builder.BooleanBuilder
	binary  *builder.BinaryBuilder
	string  *builder.StringBuilder
}

type ValueBuilder struct {
	builder *builder.SparseUnionBuilder
	i64     *builder.Int64Builder
	f64     *builder.Float64Builder
	bool    *builder.BooleanBuilder
	binary  *builder.BinaryBuilder
	string  *builder.StringBuilder
}

func NewRootBuilderFrom(recordBuilder *builder.RecordBuilderExt) *RootBuilder {
	b := &RootBuilder{recordBuilder: recordBuilder}
	b.init()
	return b
}

func (b *RootBuilder) init() {
	b.builder = b.recordBuilder.StructBuilder(Root)
	b.timestamp = b.builder.TimestampBuilder(Timestamp)
	b.u8 = b.builder.Uint8Builder(U8)
	b.u64 = b.builder.Uint64Builder(U64)
	b.i64 = b.builder.Int64Builder(I64)
	b.bool = b.builder.BooleanBuilder(Bool)
	b.binary = b.builder.BinaryBuilder(Binary)
	b.u32 = b.builder.Uint32Builder(U32)
	b.i32 = b.builder.Int32Builder(I32)
	b.string = b.builder.StringBuilder(String)
	b.values = NewValuesBuilder(b.builder.ListBuilder(Values))
	b.fixedSize8 = b.builder.FixedSizeBinaryBuilder(FixedSize8Binary)
	b.fixedSize16 = b.builder.FixedSizeBinaryBuilder(FixedSize16Binary)
	b.hmap = NewHMapBuilder(b.builder.MapBuilder(Map))
}

func (b *RootBuilder) AppendData(data *RootData) arrow.Record {
	for {
		b.Append(data)

		if b.recordBuilder.SchemaUpdateRequestCount() == 0 {
			return b.recordBuilder.NewRecord()
		}
		b.recordBuilder.UpdateSchema()
		b.init()
	}
}

func (b *RootBuilder) Append(data *RootData) {
	b.builder.Append(data, func() {
		b.timestamp.Append(data.timestamp)
		b.u8.AppendNonZero(data.u8)
		b.u64.AppendNonZero(data.u64)
		b.i64.Append(data.i64)
		b.bool.Append(data.bool)
		b.binary.Append(data.binary)
		b.u32.AppendNonZero(data.u32)
		b.i32.Append(data.i32)
		b.string.Append(data.string)
		b.values.Append(data.values)
		b.fixedSize8.Append(data.fixedSize8[:])
		b.fixedSize16.Append(data.fixedSize16[:])
		b.hmap.Append(data.hmap)
	})
}

func NewValuesBuilder(builder *builder.ListBuilder) *ValuesBuilder {
	b := &ValuesBuilder{
		builder: builder,
		values:  NewValueBuilder(builder.SparseUnionBuilder()),
	}
	return b
}

func NewHMapBuilder(builder *builder.MapBuilder) *HMapBuilder {
	valuesBuilder := builder.ItemSparseUnionBuilder()
	b := &HMapBuilder{
		builder: builder,
		keys:    builder.KeyStringBuilder(),
		values:  valuesBuilder,
		i64:     valuesBuilder.Int64Builder(I64Code),
		f64:     valuesBuilder.Float64Builder(F64Code),
		bool:    valuesBuilder.BooleanBuilder(BoolCode),
		binary:  valuesBuilder.BinaryBuilder(BinaryCode),
		string:  valuesBuilder.StringBuilder(StringCode),
	}
	return b
}

func (b *HMapBuilder) Append(data map[string]ValueData) {
	b.builder.Append(len(data), func() {
		for k, v := range data {
			b.keys.Append(k)
			if v.IsI64() {
				b.values.Append(I64Code)
				b.i64.Append(v.I64())
				b.f64.AppendNull()
				b.bool.AppendNull()
				b.binary.AppendNull()
				b.string.AppendNull()
			} else {
				b.values.Append(F64Code)
				b.f64.Append(v.F64())
				b.i64.AppendNull()
				b.bool.AppendNull()
				b.binary.AppendNull()
				b.string.AppendNull()
			}
		}
	})
}

func (b *ValuesBuilder) Append(data []ValueData) {
	b.builder.Append(len(data), func() {
		for _, v := range data {
			b.values.Append(v)
		}
	})
}

func NewValueBuilder(builder *builder.SparseUnionBuilder) *ValueBuilder {
	b := &ValueBuilder{
		builder: builder,
		i64:     builder.Int64Builder(I64Code),
		f64:     builder.Float64Builder(F64Code),
		bool:    builder.BooleanBuilder(BoolCode),
		binary:  builder.BinaryBuilder(BinaryCode),
		string:  builder.StringBuilder(StringCode),
	}
	return b
}

func (b *ValueBuilder) Append(data ValueData) {
	if data.IsI64() {
		b.builder.Append(I64Code)
		b.i64.Append(data.I64())
		b.f64.AppendNull()
		b.bool.AppendNull()
		b.binary.AppendNull()
		b.string.AppendNull()
	} else {
		b.builder.Append(F64Code)
		b.f64.Append(data.F64())
		b.i64.AppendNull()
		b.bool.AppendNull()
		b.binary.AppendNull()
		b.string.AppendNull()
	}
}
