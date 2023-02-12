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

import (
	"math"

	"github.com/apache/arrow/go/v11/arrow"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/config"
)

const DictIdKey = "dictId"

// DictionaryField is a FieldTransform that transforms dictionary fields to
// a given index type.
// If the index type is nil, the dictionary is downgraded to its value type.
type DictionaryField struct {
	// Dictionary ID
	DictID string

	// cardinality of the dictionary used to determine dictionary overflow
	cardinality uint64

	indexMaxCard []uint64
	indexTypes   []arrow.DataType
	currentIndex int
}

func NewDictionaryField(dictID string, config *builder.DictionaryConfig) *DictionaryField {
	df := DictionaryField{
		DictID:      dictID,
		cardinality: 0,
	}
	df.initIndices(config)
	return &df
}

func (t *DictionaryField) SetCardinality(card uint64) {
	t.cardinality = card
	t.updateIndexType()
}

func (t *DictionaryField) Transform(field *arrow.Field) *arrow.Field {
	if t.indexTypes == nil {
		// No index type defined, so the dictionary is downgraded to its
		// value type.
		return &arrow.Field{Name: field.Name, Type: field.Type.(*arrow.DictionaryType).ValueType, Nullable: field.Nullable, Metadata: field.Metadata}
	} else {
		// Add the dictionary ID to the metadata to ease the process checking
		// dictionary overflow.
		keys := append(field.Metadata.Keys(), DictIdKey)
		values := append(field.Metadata.Values(), t.DictID)
		metadataWithDictId := arrow.NewMetadata(keys, values)

		switch field.Type.(type) {
		case *arrow.DictionaryType:
			// Index type defined, so the dictionary is upgraded to the given
			// index type.
			dictType := &arrow.DictionaryType{
				IndexType: t.indexTypes[t.currentIndex],
				ValueType: field.Type.(*arrow.DictionaryType).ValueType,
				Ordered:   false,
			}
			return &arrow.Field{Name: field.Name, Type: dictType, Nullable: field.Nullable, Metadata: metadataWithDictId}
		default:
			// Index type defined, so field is converted to a dictionary.
			dictType := &arrow.DictionaryType{
				IndexType: t.indexTypes[t.currentIndex],
				ValueType: field.Type,
				Ordered:   false,
			}
			return &arrow.Field{Name: field.Name, Type: dictType, Nullable: field.Nullable, Metadata: metadataWithDictId}
		}
	}
}

func (t *DictionaryField) updateIndexType() {
	if t.indexTypes == nil {
		return
	}

	for t.currentIndex < len(t.indexTypes) && t.cardinality > t.indexMaxCard[t.currentIndex] {
		t.currentIndex++
	}
	if t.currentIndex >= len(t.indexTypes) {
		t.indexTypes = nil
		t.indexMaxCard = nil
		t.currentIndex = 0
	}
}

func (t *DictionaryField) initIndices(config *builder.DictionaryConfig) {
	t.indexTypes = nil
	t.indexMaxCard = nil
	t.currentIndex = 0

	if config == nil {
		return
	}

	if config.MaxCard <= math.MaxUint64 {
		t.indexTypes = []arrow.DataType{arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64}
		t.indexMaxCard = []uint64{math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64}
		return
	}

	if config.MaxCard <= math.MaxUint32 {
		t.indexTypes = []arrow.DataType{arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32}
		t.indexMaxCard = []uint64{math.MaxUint8, math.MaxUint16, math.MaxUint32}
		return
	}

	if config.MaxCard <= math.MaxUint16 {
		t.indexTypes = []arrow.DataType{arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16}
		t.indexMaxCard = []uint64{math.MaxUint8, math.MaxUint16}
		return
	}

	if config.MaxCard <= math.MaxUint8 {
		t.indexTypes = []arrow.DataType{arrow.PrimitiveTypes.Uint8}
		t.indexMaxCard = []uint64{math.MaxUint8}
		return
	}
}
