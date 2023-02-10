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
	name       string
	transforms []FieldTransform
	Children   []*TransformNode
}

// NewTransformTreeFrom creates a transformation tree from a prototype schema.
// The dictIndexType and the field metadata are used to transform the prototype
// schema into the target schema.
//
// Optional fields:
// By default all fields marked as optional in the prototype schema are removed
// from the target schema. This behavior can be changed if data is available for
// this field.
//
// Dictionary fields:
// By default all fields marked as dictionary fields in the prototype schema are
// converted to their dictionary representation. This behavior can be changed if
// the number of unique values is higher than the size of dictIndexType.
// If dictIndexType is nil, then fields marked as dictionary fields are not
// converted to their dictionary representation.
func NewTransformTreeFrom(prototype *arrow.Schema, dictIndexType arrow.DataType) *TransformNode {
	protoFields := prototype.Fields()
	rootTNode := TransformNode{Children: make([]*TransformNode, 0, len(protoFields))}

	for i := 0; i < len(protoFields); i++ {
		rootTNode.Children = append(rootTNode.Children, newTransformNodeFrom(&protoFields[i], dictIndexType))
	}

	return &rootTNode
}

func newTransformNodeFrom(prototype *arrow.Field, dictIndexType arrow.DataType) *TransformNode {
	var transforms []FieldTransform

	// Check if the field is optional and if so, remove it by emitting a
	// NoField transformation.
	metadata := prototype.Metadata
	keyIdx := metadata.FindKey(OptionalKey)
	if keyIdx != -1 {
		transforms = append(transforms, &transform2.NoField{})
	}

	// Check if the field is a dictionary field and if so, convert it to its
	// dictionary representation by emitting a DictionaryField transformation.
	keyIdx = metadata.FindKey(DictionaryKey)
	if keyIdx != -1 {
		transforms = append(transforms, &transform2.DictionaryField{IndexType: dictIndexType})
	}

	// If no transformation was added, then add an Identity transformation.
	if len(transforms) == 0 {
		transforms = append(transforms, &transform2.IdentityField{})
	}

	node := TransformNode{name: prototype.Name, transforms: transforms}

	switch dt := prototype.Type.(type) {
	case *arrow.DictionaryType:
		node.transforms = append(node.transforms, &transform2.DictionaryField{IndexType: dictIndexType})
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

	for _, transform := range t.transforms {
		if _, ok := transform.(*transform2.NoField); !ok {
			t.transforms[n] = transform
			n++
		}
	}

	t.transforms = t.transforms[:n]
}
