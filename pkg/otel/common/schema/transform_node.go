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

package schema

import (
	"github.com/apache/arrow/go/v11/arrow"

	transform2 "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/transform"
)

// FieldTransform is an interface to apply a transformation to a field.
type FieldTransform interface {
	Transform(field *arrow.Field) *arrow.Field
}

// TransformNode is a node in a transformation tree.
// It can be a leaf node or a node with children.
type TransformNode struct {
	name            string
	transformations []FieldTransform
	Children        []*TransformNode
}

// NewTransformTreeFrom creates a transformation tree from a prototype schema.
// The dictIndexType and the field metadata are used to transform the prototype
// schema into the target schema.
//
// If dictIndexType is nil, then dictionary fields are downgraded to their
// value type.
//
// The metadata #optionial is used to remove fields from the prototype schema.
// Removed fields can be added back by changing the transformation tree.
func NewTransformTreeFrom(prototype *arrow.Schema, dictIndexType arrow.DataType) *TransformNode {
	protoFields := prototype.Fields()
	rootTNode := TransformNode{Children: make([]*TransformNode, 0, len(protoFields))}

	for i := 0; i < len(protoFields); i++ {
		rootTNode.Children = append(rootTNode.Children, newTransformNodeFrom(&protoFields[i], dictIndexType))
	}

	return &rootTNode
}

func newTransformNodeFrom(prototype *arrow.Field, dictIndexType arrow.DataType) *TransformNode {
	var transform FieldTransform

	transform = &transform2.IdentityField{}

	// Check if the field is optional and if so, remove it by emitting a
	// NoField transformation.
	metadata := prototype.Metadata
	keyIdx := metadata.FindKey(OptionalKey)
	if keyIdx != -1 {
		transform = &transform2.NoField{}
	}

	node := TransformNode{name: prototype.Name, transformations: []FieldTransform{transform}}

	switch dt := prototype.Type.(type) {
	case *arrow.DictionaryType:
		node.transformations = append(node.transformations, &transform2.DictionaryField{IndexType: dictIndexType})
	case *arrow.StructType:
		node.Children = make([]*TransformNode, 0, len(dt.Fields()))
		for _, child := range prototype.Type.(*arrow.StructType).Fields() {
			node.Children = append(node.Children, newTransformNodeFrom(&child, dictIndexType))
		}
	case *arrow.ListType:
		elemField := dt.ElemField()
		node.Children = make([]*TransformNode, 0, 1)
		node.Children = append(node.Children, newTransformNodeFrom(&elemField, dictIndexType))
	case arrow.UnionType:
		node.Children = make([]*TransformNode, 0, len(dt.Fields()))
		for _, child := range dt.Fields() {
			node.Children = append(node.Children, newTransformNodeFrom(&child, dictIndexType))
		}
	case *arrow.MapType:
		node.Children = make([]*TransformNode, 0, 2)
		keyField := dt.KeyField()
		node.Children = append(node.Children, newTransformNodeFrom(&keyField, dictIndexType))
		valueField := dt.ItemField()
		node.Children = append(node.Children, newTransformNodeFrom(&valueField, dictIndexType))
	}

	return &node
}

func (t *TransformNode) RemoveOptional() {
	n := 0

	for _, transform := range t.transformations {
		if _, ok := transform.(*transform2.NoField); !ok {
			t.transformations[n] = transform
			n++
		}
	}

	t.transformations = t.transformations[:n]
}
