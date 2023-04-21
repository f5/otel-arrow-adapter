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

package otlp

import (
	"github.com/apache/arrow/go/v12/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"

	arrowutils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

type (
	// AttributeIDs is a struct containing the Arrow field IDs of the
	// attributes.
	AttributeIDs struct {
		ID    int
		Key   int
		value int
	}

	// AttributeMapStore is a store for attributes.
	// The attributes are stored in a map by ID.
	AttributeMapStore struct {
		attributesByID map[uint16]*pcommon.Map
	}
)

// AttributesByID returns the attributes for the given ID.
func (s *AttributeMapStore) AttributesByID(id uint16) *pcommon.Map {
	if m, ok := s.attributesByID[id]; ok {
		return m
	}
	return nil
}

// AttributeMapStoreFrom creates an AttributeMapStore from an arrow.Record.
// Note: This function consume the record.
func AttributeMapStoreFrom(record arrow.Record) (*AttributeMapStore, error) {
	defer record.Release()

	store := &AttributeMapStore{
		attributesByID: make(map[uint16]*pcommon.Map),
	}

	attrIDS, err := SchemaToAttributeIDs(record.Schema())
	if err != nil {
		return nil, werror.Wrap(err)
	}

	attrsCount := int(record.NumRows())

	// Read all key/value tuples from the record and reconstruct the attributes
	// map by ID.
	for i := 0; i < attrsCount; i++ {
		ID, err := arrowutils.U16FromRecord(record, attrIDS.ID, i)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		key, err := arrowutils.StringFromRecord(record, attrIDS.Key, i)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		arrValue, err := arrowutils.SparseUnionFromRecord(record, attrIDS.value, i)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		value := pcommon.NewValueEmpty()
		if err := UpdateValueFrom(value, arrValue, i); err != nil {
			return nil, werror.Wrap(err)
		}

		m, ok := store.attributesByID[ID]
		if !ok {
			newMap := pcommon.NewMap()
			m = &newMap
			store.attributesByID[ID] = m
		}
		value.CopyTo(m.PutEmpty(key))
	}

	return store, nil
}

// SchemaToAttributeIDs pre-computes the field IDs for the attributes record.
func SchemaToAttributeIDs(schema *arrow.Schema) (*AttributeIDs, error) {
	ID, err := arrowutils.FieldIDFromSchema(schema, constants.ID)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	key, err := arrowutils.FieldIDFromSchema(schema, constants.AttrsRecordKey)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	value, err := arrowutils.FieldIDFromSchema(schema, constants.AttrsRecordValue)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &AttributeIDs{
		ID:    ID,
		Key:   key,
		value: value,
	}, nil
}
