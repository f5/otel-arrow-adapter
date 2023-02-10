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

package builder

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
)

// RecordBuilderExt is a wrapper around array.RecordBuilder that allows for
// schema updates to be requested during the construction of a record.

type RecordBuilderExt struct {
	allocator memory.Allocator

	// The record builder that is being wrapped.
	recordBuilder *array.RecordBuilder

	// The prototype schema initially defined to build the AdaptiveSchema.
	protoSchema *arrow.Schema

	// The transform tree that is used to build the final schema from the
	// prototype schema after applying the transformation tree.
	transformTree *schema.TransformNode

	// The pending schema update requests.
	updateRequest *SchemaUpdateRequest
}

type SchemaUpdateRequest struct {
	count int
}

// NewRecordBuilderExt creates a new RecordBuilderExt from the given allocator
// and a prototype schema.
func NewRecordBuilderExt(allocator memory.Allocator, protoSchema *arrow.Schema) *RecordBuilderExt {
	transformTree := schema.NewTransformTreeFrom(protoSchema, arrow.PrimitiveTypes.Uint32)
	schema := schema.NewSchemaFrom(protoSchema, transformTree)
	recordBuilder := array.NewRecordBuilder(allocator, schema)

	return &RecordBuilderExt{
		allocator:     allocator,
		recordBuilder: recordBuilder,
		protoSchema:   protoSchema,
		transformTree: transformTree,
		updateRequest: &SchemaUpdateRequest{count: 0},
	}
}

func (rb *RecordBuilderExt) Release() {
	rb.recordBuilder.Release()
}

// RecordBuilder returns the underlying array.RecordBuilder.
func (rb *RecordBuilderExt) RecordBuilder() *array.RecordBuilder {
	return rb.recordBuilder
}

// NewRecord returns a new record from the underlying array.RecordBuilder.
func (rb *RecordBuilderExt) NewRecord() arrow.Record {
	return rb.recordBuilder.NewRecord()
}

func (rb *RecordBuilderExt) SchemaUpdateRequestCount() int {
	return rb.updateRequest.count
}

func (rb *RecordBuilderExt) SchemaUpdateRequestReset() {
	rb.updateRequest.count = 0
}

func (rb *RecordBuilderExt) builder(name string) array.Builder {
	// Retrieve the builder for the field.
	schema := rb.recordBuilder.Schema()
	fieldIndices := schema.FieldIndices(name)

	if len(fieldIndices) == 1 {
		return rb.recordBuilder.Field(fieldIndices[0])
	}
	return nil
}

// UpdateSchema updates the schema based on the pending schema update requests
// the initial prototype schema.
func (rb *RecordBuilderExt) UpdateSchema() {
	schema := schema.NewSchemaFrom(rb.protoSchema, rb.transformTree)
	rb.recordBuilder.Release()
	rb.recordBuilder = array.NewRecordBuilder(rb.allocator, schema)
	rb.updateRequest.count = 0
}

func (rb *RecordBuilderExt) protoDataTypeAndTransformNode(name string) (arrow.DataType, *schema.TransformNode) {
	// Retrieve the transform node for the field.
	protoFieldIndices := rb.protoSchema.FieldIndices(name)

	if len(protoFieldIndices) == 0 {
		// The field doesn't exist in the proto schema so we panic because this
		// is a programming error.
		panic(fmt.Sprintf("field %q not found in the proto schema", name))
	}

	if len(protoFieldIndices) > 1 {
		// The field is ambiguous in the proto schema so we panic because this
		// is a programming error.
		panic(fmt.Sprintf("field %q is ambiguous in the proto schema", name))
	}

	return rb.protoSchema.Field(protoFieldIndices[0]).Type, rb.transformTree.Children[protoFieldIndices[0]]
}

// TimestampBuilder returns a TimestampBuilder wrapper for the field with the
// given name. If the underlying builder doesn't exist, an empty wrapper is
// returned, so that the feeding process can continue without panicking. This
// is useful to handle optional fields.
func (rb *RecordBuilderExt) TimestampBuilder(name string) *TimestampBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &TimestampBuilder{builder: builder.(*array.TimestampBuilder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &TimestampBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// FixedSizeBinaryBuilder returns a FixedSizeBinaryBuilder wrapper for the
// field with the given name. If the underlying builder doesn't exist, an empty
// wrapper is returned, so that the feeding process can continue without
// panicking. This is useful to handle optional fields.
func (rb *RecordBuilderExt) FixedSizeBinaryBuilder(name string) *FixedSizeBinaryBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &FixedSizeBinaryBuilder{builder: builder, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &FixedSizeBinaryBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// MapBuilder returns a MapBuilder wrapper for the field with the given name.
// If the underlying builder doesn't exist, an empty wrapper is returned, so
// that the feeding process can continue without panicking. This is useful to
// handle optional fields.
func (rb *RecordBuilderExt) MapBuilder(name string) *MapBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &MapBuilder{builder: builder.(*array.MapBuilder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &MapBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// StringBuilder returns a StringBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) StringBuilder(name string) *StringBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &StringBuilder{builder: builder, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &StringBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// BooleanBuilder returns a BooleanBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) BooleanBuilder(name string) *BooleanBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &BooleanBuilder{builder: builder.(*array.BooleanBuilder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &BooleanBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// BinaryBuilder returns a BinaryBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) BinaryBuilder(name string) *BinaryBuilder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &BinaryBuilder{builder: builder, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &BinaryBuilder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Uint8Builder returns a Uint8Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Uint8Builder(name string) *Uint8Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &Uint8Builder{builder: builder, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Uint8Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Uint32Builder returns a Uint32Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Uint32Builder(name string) *Uint32Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &Uint32Builder{builder: builder, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Uint32Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Uint64Builder returns a Uint64Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Uint64Builder(name string) *Uint64Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &Uint64Builder{builder: builder, transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Uint64Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Int32Builder returns a Int32Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Int32Builder(name string) *Int32Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &Int32Builder{builder: builder.(*array.Int32Builder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Int32Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Int64Builder returns a Int64Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Int64Builder(name string) *Int64Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &Int64Builder{builder: builder.(*array.Int64Builder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Int64Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// Float64Builder returns a Float64Builder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) Float64Builder(name string) *Float64Builder {
	_, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &Float64Builder{builder: builder.(*array.Float64Builder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &Float64Builder{builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// StructBuilder returns a StructBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) StructBuilder(name string) *StructBuilder {
	protoDataType, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder != nil {
		return &StructBuilder{protoDataType: protoDataType.(*arrow.StructType), builder: builder.(*array.StructBuilder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &StructBuilder{protoDataType: protoDataType.(*arrow.StructType), builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}

// ListBuilder returns a ListBuilder wrapper for the field with the given
// name. If the underlying builder doesn't exist, an empty wrapper is returned,
// so that the feeding process can continue without panicking. This is useful
// to handle optional fields.
func (rb *RecordBuilderExt) ListBuilder(name string) *ListBuilder {
	protoDataType, transformNode := rb.protoDataTypeAndTransformNode(name)
	builder := rb.builder(name)

	if builder == nil {
		return &ListBuilder{protoDataType: protoDataType.(*arrow.ListType), builder: builder.(*array.ListBuilder), transformNode: transformNode, updateRequest: rb.updateRequest}
	} else {
		return &ListBuilder{protoDataType: protoDataType.(*arrow.ListType), builder: nil, transformNode: transformNode, updateRequest: rb.updateRequest}
	}
}
