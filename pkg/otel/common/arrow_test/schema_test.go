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
	"github.com/davecgh/go-spew/spew"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
)

const (
	Root      = "root"
	Count     = "count"
	SchemaUrl = "schema_url"
	Values    = "values"
	I64       = "i64"
	F64       = "f64"
)

const (
	I64Code int8 = 0
	F64Code int8 = 1
)

var (
	valueDT = arrow.SparseUnionOf(
		[]arrow.Field{
			{Name: I64, Type: arrow.PrimitiveTypes.Int64, Metadata: acommon.OptionalField},
			{Name: F64, Type: arrow.PrimitiveTypes.Float64, Metadata: acommon.OptionalField},
		},
		[]arrow.UnionTypeCode{
			I64Code,
			F64Code,
		})

	protoSchema = arrow.NewSchema([]arrow.Field{
		{Name: Root, Type: arrow.StructOf([]arrow.Field{
			{Name: Count, Type: arrow.PrimitiveTypes.Uint8, Metadata: acommon.OptionalField},
			{Name: SchemaUrl, Type: arrow.BinaryTypes.String, Metadata: acommon.OptionalField},
			{Name: Values, Type: arrow.ListOf(valueDT), Metadata: acommon.OptionalField},
		}...)},
	}, nil)
)

func TestTransformationTree(t *testing.T) {
	transformTree := acommon.NewTransformTreeFrom(protoSchema, arrow.PrimitiveTypes.Uint8)

	spew.Dump(transformTree)
}

func TestSchema(t *testing.T) {
	recordBuilderExt := builder.NewRecordBuilderExt(memory.NewGoAllocator(), protoSchema)

	rootData := RootData{
		count:  1,
		schema: "test",
		values: []ValueData{
			I64ValueData{1},
			F64ValueData{2.0},
		},
	}

	for {
		rootBuilder := NewRootBuilderFrom(recordBuilderExt)
		rootBuilder.Append(&rootData)

		if recordBuilderExt.SchemaUpdateRequestCount() == 0 {
			record := recordBuilderExt.NewRecord()
			spew.Dump(record)
			break
		}
		recordBuilderExt.UpdateSchema()
	}
}

type RootData struct {
	count  uint8
	schema string
	values []ValueData
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
	builder *builder.StructBuilder
	count   *builder.Uint8Builder
	schema  *builder.StringBuilder
	values  *ValuesBuilder
}

type ValuesBuilder struct {
	builder *builder.ListBuilder
	values  *ValueBuilder
}

type ValueBuilder struct {
	builder *builder.SparseUnionBuilder
	i64     *builder.Int64Builder
	f64     *builder.Float64Builder
}

func NewRootBuilderFrom(recordBuilder *builder.RecordBuilderExt) *RootBuilder {
	rootBuilder := recordBuilder.StructBuilder(Root)
	b := &RootBuilder{
		builder: rootBuilder,
		count:   rootBuilder.Uint8Builder(Count),
		schema:  rootBuilder.StringBuilder(SchemaUrl),
		values:  NewValuesBuilder(rootBuilder.ListBuilder(Values)),
	}
	return b
}

func (b *RootBuilder) Append(data *RootData) {
	if data == nil {
		b.builder.AppendNull()
		return
	}
	b.builder.AppendStruct()
	b.count.AppendNonZero(data.count)
	b.schema.Append(data.schema)
	b.values.Append(data.values)
}

func NewValuesBuilder(builder *builder.ListBuilder) *ValuesBuilder {
	b := &ValuesBuilder{
		builder: builder,
		values:  NewValueBuilder(builder.SparseUnionBuilder()),
	}
	return b
}

func (b *ValuesBuilder) Append(data []ValueData) {
	if data == nil || len(data) == 0 {
		b.builder.AppendNull()
		return
	}
	b.builder.AppendNItems(len(data))
	for _, v := range data {
		b.values.Append(v)
	}
}

func NewValueBuilder(builder *builder.SparseUnionBuilder) *ValueBuilder {
	b := &ValueBuilder{
		builder: builder,
		i64:     builder.Int64Builder(I64Code),
		f64:     builder.Float64Builder(F64Code),
	}
	return b
}

func (b *ValueBuilder) Append(data ValueData) {
	if data.IsI64() {
		b.builder.AppendSparseUnion(I64Code)
		b.i64.Append(data.I64())
		b.f64.AppendNull()
	} else {
		b.builder.AppendSparseUnion(F64Code)
		b.f64.Append(data.F64())
		b.i64.AppendNull()
	}
}
