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

	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
)

type SparseUnionBuilder struct {
	protoDataType *arrow.SparseUnionType
	builder       *array.SparseUnionBuilder
	transformNode *schema.TransformNode
	updateRequest *SchemaUpdateRequest
}

func (sub *SparseUnionBuilder) protoDataTypeAndTransformNode(childCode arrow.UnionTypeCode) (arrow.DataType, *schema.TransformNode) {
	for i, code := range sub.protoDataType.TypeCodes() {
		if code == childCode {
			return sub.protoDataType.Fields()[i].Type, sub.transformNode.Children[i]
		}
	}

	panic(fmt.Sprintf("child code %d not found in the proto schema", childCode))
}

func (sub *SparseUnionBuilder) getBuilder(childCode arrow.UnionTypeCode) array.Builder {
	if sub.builder == nil {
		return nil
	}
	structType := sub.builder.Type().(*arrow.SparseUnionType)
	for i, code := range structType.TypeCodes() {
		if code == childCode {
			return sub.builder.Child(i)
		}
	}
	return nil
}

func (sub *SparseUnionBuilder) Int64Builder(code arrow.UnionTypeCode) *Int64Builder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &Int64Builder{builder: builder.(*array.Int64Builder), transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &Int64Builder{builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

func (sub *SparseUnionBuilder) Float64Builder(code arrow.UnionTypeCode) *Float64Builder {
	builder := sub.getBuilder(code)
	_, transformNode := sub.protoDataTypeAndTransformNode(code)

	if builder != nil {
		return &Float64Builder{builder: builder.(*array.Float64Builder), transformNode: transformNode, updateRequest: sub.updateRequest}
	} else {
		return &Float64Builder{builder: nil, transformNode: transformNode, updateRequest: sub.updateRequest}
	}
}

func (sub *SparseUnionBuilder) AppendNull() {
	sub.builder.AppendNull()
}

func (sub *SparseUnionBuilder) AppendSparseUnion(code int8) {
	if sub.builder != nil {
		sub.builder.Append(code)
		return
	}

	// If the builder is nil, then the transform node is not optional.
	sub.transformNode.RemoveOptional()
	sub.updateRequest.count++
}
