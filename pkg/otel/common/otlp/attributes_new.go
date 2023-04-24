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

	// Attributes16Store is a store for attributes.
	// The attributes are stored in a map by ID.
	Attributes16Store struct {
		lastID         uint16
		attributesByID map[uint16]*pcommon.Map
	}

	// Attributes32Store is a store for attributes.
	// The attributes are stored in a map by ID.
	Attributes32Store struct {
		lastID         uint32
		attributesByID map[uint32]*pcommon.Map
	}
)

// NewAttributes16Store creates a new Attributes16Store.
func NewAttributes16Store() *Attributes16Store {
	return &Attributes16Store{
		attributesByID: make(map[uint16]*pcommon.Map),
	}
}

// NewAttributes32Store creates a new Attributes32Store.
func NewAttributes32Store() *Attributes32Store {
	return &Attributes32Store{
		attributesByID: make(map[uint32]*pcommon.Map),
	}
}

// AttributesByDeltaID returns the attributes for the given Delta ID.
func (s *Attributes16Store) AttributesByDeltaID(ID uint16) *pcommon.Map {
	s.lastID += ID
	if m, ok := s.attributesByID[s.lastID]; ok {
		return m
	}
	return nil
}

// AttributesByID returns the attributes for the given ID.
func (s *Attributes16Store) AttributesByID(ID uint16) *pcommon.Map {
	if m, ok := s.attributesByID[ID]; ok {
		return m
	}
	return nil
}

// AttributesByDeltaID returns the attributes for the given Delta ID.
func (s *Attributes32Store) AttributesByDeltaID(ID uint32) *pcommon.Map {
	s.lastID += ID
	if m, ok := s.attributesByID[s.lastID]; ok {
		return m
	}
	return nil
}

// Attributes16StoreFrom creates an Attribute16Store from an arrow.Record.
// Note: This function consume the record.
func Attributes16StoreFrom(record arrow.Record, store *Attributes16Store) error {
	defer record.Release()

	attrIDS, err := SchemaToAttributeIDs(record.Schema())
	if err != nil {
		return werror.Wrap(err)
	}

	attrsCount := int(record.NumRows())

	// Read all key/value tuples from the record and reconstruct the attributes
	// map by ID.
	for i := 0; i < attrsCount; i++ {
		ID, err := arrowutils.U16FromRecord(record, attrIDS.ID, i)
		if err != nil {
			return werror.Wrap(err)
		}

		key, err := arrowutils.StringFromRecord(record, attrIDS.Key, i)
		if err != nil {
			return werror.Wrap(err)
		}

		arrValue, err := arrowutils.SparseUnionFromRecord(record, attrIDS.value, i)
		if err != nil {
			return werror.Wrap(err)
		}

		value := pcommon.NewValueEmpty()
		if err := UpdateValueFrom(value, arrValue, i); err != nil {
			return werror.Wrap(err)
		}

		m, ok := store.attributesByID[ID]
		if !ok {
			newMap := pcommon.NewMap()
			m = &newMap
			store.attributesByID[ID] = m
		}
		value.CopyTo(m.PutEmpty(key))
	}

	return nil
}

// Attributes32StoreFrom creates an Attributes32Store from an arrow.Record.
// Note: This function consume the record.
func Attributes32StoreFrom(record arrow.Record, store *Attributes32Store) error {
	defer record.Release()

	attrIDS, err := SchemaToAttributeIDs(record.Schema())
	if err != nil {
		return werror.Wrap(err)
	}

	attrsCount := int(record.NumRows())

	// Read all key/value tuples from the record and reconstruct the attributes
	// map by ID.
	for i := 0; i < attrsCount; i++ {
		ID, err := arrowutils.U32FromRecord(record, attrIDS.ID, i)
		if err != nil {
			return werror.Wrap(err)
		}

		key, err := arrowutils.StringFromRecord(record, attrIDS.Key, i)
		if err != nil {
			return werror.Wrap(err)
		}

		arrValue, err := arrowutils.SparseUnionFromRecord(record, attrIDS.value, i)
		if err != nil {
			return werror.Wrap(err)
		}

		value := pcommon.NewValueEmpty()
		if err := UpdateValueFrom(value, arrValue, i); err != nil {
			return werror.Wrap(err)
		}

		m, ok := store.attributesByID[ID]
		if !ok {
			newMap := pcommon.NewMap()
			m = &newMap
			store.attributesByID[ID] = m
		}
		value.CopyTo(m.PutEmpty(key))
	}

	return nil
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
