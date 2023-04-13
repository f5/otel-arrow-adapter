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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/update"
)

type StructBuilder struct {
	protoDataType *arrow.StructType
	builder       *array.StructBuilder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

func (sb *StructBuilder) protoDataTypeAndTransformNode(name string) (arrow.DataType, *schema.TransformNode) {
	// Retrieve the transform node for the field.
	protoFieldIdx, found := sb.protoDataType.FieldIdx(name)

	if !found {
		panic(fmt.Sprintf("field %q not found in the proto schema", name))
	}

	return sb.protoDataType.Field(protoFieldIdx).Type, sb.transformNode.Children[protoFieldIdx]
}

func (sb *StructBuilder) getBuilder(name string) array.Builder {
	if sb.builder == nil {
		return nil
	}

	structType := sb.builder.Type().(*arrow.StructType)
	fieldIdx, found := structType.FieldIdx(name)

	if found {
		return sb.builder.FieldBuilder(fieldIdx)
	}
	return nil
}

func (sb *StructBuilder) TimestampBuilder(name string) *TimestampBuilder {
	timestampBuilder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if timestampBuilder != nil {
		return &TimestampBuilder{builder: timestampBuilder.(*array.TimestampBuilder), transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &TimestampBuilder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) DurationBuilder(name string) *DurationBuilder {
	builder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if builder != nil {
		return &DurationBuilder{builder: builder.(*array.DurationBuilder), transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &DurationBuilder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) StringBuilder(name string) *StringBuilder {
	stringBuilder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if stringBuilder != nil {
		return NewStringBuilder(stringBuilder, transformNode, sb.updateRequest)
	} else {
		return NewStringBuilder(nil, transformNode, sb.updateRequest)
	}
}

func (sb *StructBuilder) Uint8Builder(name string) *Uint8Builder {
	uint8Builder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if uint8Builder != nil {
		return &Uint8Builder{builder: uint8Builder, transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &Uint8Builder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) Uint32Builder(name string) *Uint32Builder {
	uint32Builder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if uint32Builder != nil {
		return &Uint32Builder{builder: uint32Builder, transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &Uint32Builder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) Uint64Builder(name string) *Uint64Builder {
	uint64Builder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if uint64Builder != nil {
		return &Uint64Builder{builder: uint64Builder, transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &Uint64Builder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) Int32Builder(name string) *Int32Builder {
	int32Builder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if int32Builder != nil {
		return &Int32Builder{builder: int32Builder, transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &Int32Builder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) Int64Builder(name string) *Int64Builder {
	int64Builder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if int64Builder != nil {
		return &Int64Builder{builder: int64Builder, transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &Int64Builder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) Float64Builder(name string) *Float64Builder {
	float64Builder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if float64Builder != nil {
		return &Float64Builder{builder: float64Builder.(*array.Float64Builder), transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &Float64Builder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) FixedSizeBinaryBuilder(name string) *FixedSizeBinaryBuilder {
	fixedSizeBinaryBuilder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if fixedSizeBinaryBuilder != nil {
		return &FixedSizeBinaryBuilder{builder: fixedSizeBinaryBuilder, transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &FixedSizeBinaryBuilder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) ListBuilder(name string) *ListBuilder {
	listBuilder := sb.getBuilder(name)
	protoDataType, transformNode := sb.protoDataTypeAndTransformNode(name)

	if listBuilder != nil {
		return &ListBuilder{protoDataType: protoDataType.(*arrow.ListType), builder: listBuilder.(*array.ListBuilder), transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &ListBuilder{protoDataType: protoDataType.(*arrow.ListType), builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) StructBuilder(name string) *StructBuilder {
	structBuilder := sb.getBuilder(name)
	protoDataType, transformNode := sb.protoDataTypeAndTransformNode(name)

	if structBuilder != nil {
		return &StructBuilder{protoDataType: protoDataType.(*arrow.StructType), builder: structBuilder.(*array.StructBuilder), transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &StructBuilder{protoDataType: protoDataType.(*arrow.StructType), builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) BooleanBuilder(name string) *BooleanBuilder {
	booleanBuilder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if booleanBuilder != nil {
		return &BooleanBuilder{builder: booleanBuilder.(*array.BooleanBuilder), transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &BooleanBuilder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) MapBuilder(name string) *MapBuilder {
	mapBuilder := sb.getBuilder(name)
	protoDataType, transformNode := sb.protoDataTypeAndTransformNode(name)

	if mapBuilder != nil {
		return &MapBuilder{
			protoDataType: protoDataType.(*arrow.MapType),
			builder:       mapBuilder.(*array.MapBuilder),
			transformNode: transformNode,
			updateRequest: sb.updateRequest,
		}
	} else {
		return &MapBuilder{
			protoDataType: protoDataType.(*arrow.MapType),
			builder:       nil,
			transformNode: transformNode,
			updateRequest: sb.updateRequest,
		}
	}
}

func (sb *StructBuilder) SparseUnionBuilder(name string) *SparseUnionBuilder {
	sparseUnionBuilder := sb.getBuilder(name)
	protoDataType, transformNode := sb.protoDataTypeAndTransformNode(name)

	if sparseUnionBuilder != nil {
		return &SparseUnionBuilder{protoDataType: protoDataType.(*arrow.SparseUnionType), builder: sparseUnionBuilder.(*array.SparseUnionBuilder), transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &SparseUnionBuilder{protoDataType: protoDataType.(*arrow.SparseUnionType), builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) BinaryBuilder(name string) *BinaryBuilder {
	binaryBuilder := sb.getBuilder(name)
	_, transformNode := sb.protoDataTypeAndTransformNode(name)

	if binaryBuilder != nil {
		return &BinaryBuilder{builder: binaryBuilder, transformNode: transformNode, updateRequest: sb.updateRequest}
	} else {
		return &BinaryBuilder{builder: nil, transformNode: transformNode, updateRequest: sb.updateRequest}
	}
}

func (sb *StructBuilder) AppendNull() {
	if sb.builder != nil {
		sb.builder.AppendNull()
	}
}

func (sb *StructBuilder) Append(data interface{}, fieldAppenders func() error) (err error) {
	if sb.builder != nil {
		if data == nil {
			sb.builder.AppendNull()
		} else {
			sb.builder.Append(true)
		}

		err = fieldAppenders()
		return
	}

	if data != nil {
		// If the builder is nil, then the transform node is not optional.
		sb.transformNode.RemoveOptional()
		sb.updateRequest.Inc()

		err = fieldAppenders()
	}
	return
}

func (sb *StructBuilder) NewStructArray() *array.Struct {
	if sb.builder != nil {
		return sb.builder.NewStructArray()
	}
	return nil
}

func (sb *StructBuilder) Release() {
	if sb.builder != nil {
		sb.builder.Release()
	}
}
