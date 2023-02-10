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

package transform

import "github.com/apache/arrow/go/v11/arrow"

// DictionaryField is a FieldTransform that transforms dictionary fields to
// a given index type.
// If the index type is nil, the dictionary is downgraded to its value type.
type DictionaryField struct {
	IndexType arrow.DataType
}

func (t *DictionaryField) Transform(field *arrow.Field) *arrow.Field {
	if t.IndexType == nil {
		// No index type defined, sp the dictionary is downgraded to its
		// value type.
		return &arrow.Field{Name: field.Name, Type: field.Type.(*arrow.DictionaryType).ValueType, Nullable: field.Nullable, Metadata: field.Metadata}
	} else {
		switch field.Type.(type) {
		case *arrow.DictionaryType:
			// Index type defined, so the dictionary is upgraded to the given
			// index type.
			dictType := &arrow.DictionaryType{
				IndexType: t.IndexType,
				ValueType: field.Type.(*arrow.DictionaryType).ValueType,
				Ordered:   false,
			}
			return &arrow.Field{Name: field.Name, Type: dictType, Nullable: field.Nullable, Metadata: field.Metadata}
		default:
			// Index type defined, so field is converted to a dictionary.
			dictType := &arrow.DictionaryType{
				IndexType: t.IndexType,
				ValueType: field.Type,
				Ordered:   false,
			}
			return &arrow.Field{Name: field.Name, Type: dictType, Nullable: field.Nullable, Metadata: field.Metadata}
		}
	}
}
