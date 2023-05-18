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

package arrow

// Attributes record builder for 32-bit Parent IDs.

import (
	"errors"
	"sort"

	"github.com/apache/arrow/go/v12/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"

	arrow2 "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

var (
	// AttrsSchema32 is the Arrow schema for Attributes records with 32-bit
	// Parent IDs.
	AttrsSchema32 = arrow.NewSchema([]arrow.Field{
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttrsRecordKey, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttrsRecordValue, Type: AnyValueDT},
	}, nil)

	// DeltaEncodedAttrsSchema32 is the Arrow schema for Attributes records with
	// 32-bit Parent IDs that are delta encoded.
	DeltaEncodedAttrsSchema32 = arrow.NewSchema([]arrow.Field{
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Dictionary8, schema.DeltaEncoding)},
		{Name: constants.AttrsRecordKey, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttrsRecordValue, Type: AnyValueDT},
	}, nil)
)

type (
	Attrs32Builder struct {
		released bool

		builder *builder.RecordBuilderExt // Record builder

		pib *builder.Uint32Builder
		kb  *builder.StringBuilder
		ab  *AnyValueBuilder

		accumulator *Attributes32Accumulator
		payloadType *PayloadType

		parentIdEncoding int
	}

	Attrs32ByNothing          struct{}
	Attrs32ByParentIdKeyValue struct{}
	Attrs32ByKeyParentIdValue struct{}
	Attrs32ByKeyValueParentId struct{}
)

func NewAttrs32Builder(rBuilder *builder.RecordBuilderExt, payloadType *PayloadType, sorter Attrs32Sorter) *Attrs32Builder {
	b := &Attrs32Builder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewAttributes32Accumulator(sorter),
		payloadType: payloadType,
	}
	b.init()
	return b
}

func NewDeltaEncodedAttrs32Builder(payloadType *PayloadType, rBuilder *builder.RecordBuilderExt, sorter Attrs32Sorter) *Attrs32Builder {
	b := &Attrs32Builder{
		released:         false,
		builder:          rBuilder,
		accumulator:      NewAttributes32Accumulator(sorter),
		payloadType:      payloadType,
		parentIdEncoding: ParentIdDeltaEncoding,
	}

	b.init()
	return b
}

func NewAttrs32BuilderWithEncoding(rBuilder *builder.RecordBuilderExt, payloadType *PayloadType, conf *Attrs32Config) *Attrs32Builder {
	b := &Attrs32Builder{
		released:         false,
		builder:          rBuilder,
		accumulator:      NewAttributes32Accumulator(conf.Sorter),
		payloadType:      payloadType,
		parentIdEncoding: conf.ParentIdEncoding,
	}

	b.init()
	return b
}

func (b *Attrs32Builder) init() {
	b.pib = b.builder.Uint32Builder(constants.ParentID)
	b.kb = b.builder.StringBuilder(constants.AttrsRecordKey)
	b.ab = AnyValueBuilderFrom(b.builder.SparseUnionBuilder(constants.AttrsRecordValue))
}

func (b *Attrs32Builder) Accumulator() *Attributes32Accumulator {
	return b.accumulator
}

func (b *Attrs32Builder) TryBuild() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(ErrBuilderAlreadyReleased)
	}

	prevParentID := uint32(0)
	prevKey := ""
	prevValue := pcommon.NewValueEmpty()
	b.accumulator.Sort()

	for _, attr := range b.accumulator.attrs {
		switch b.parentIdEncoding {
		case ParentIdNoEncoding:
			b.pib.Append(attr.ParentID)
		case ParentIdDeltaEncoding:
			delta := attr.ParentID - prevParentID
			prevParentID = attr.ParentID
			b.pib.Append(delta)
		case ParentIdDeltaGroupEncoding:
			if prevKey == attr.Key && Equal(prevValue, attr.Value) {
				delta := attr.ParentID - prevParentID
				prevParentID = attr.ParentID
				b.pib.Append(delta)
			} else {
				prevKey = attr.Key
				prevValue = attr.Value
				prevParentID = attr.ParentID
				b.pib.Append(attr.ParentID)
			}
		}

		b.kb.Append(attr.Key)
		if err := b.ab.Append(attr.Value); err != nil {
			return nil, werror.Wrap(err)
		}
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}

	return
}

func (b *Attrs32Builder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *Attrs32Builder) Build() (arrow.Record, error) {
	schemaNotUpToDateCount := 0

	var record arrow.Record
	var err error

	// Loop until the record is built successfully.
	// Intermediaries steps may be required to update the schema.
	for {
		record, err = b.TryBuild()
		if err != nil {
			if record != nil {
				record.Release()
			}

			switch {
			case errors.Is(err, schema.ErrSchemaNotUpToDate):
				schemaNotUpToDateCount++
				if schemaNotUpToDateCount > 5 {
					panic("Too many consecutive schema updates. This shouldn't happen.")
				}
			default:
				return nil, werror.Wrap(err)
			}
		} else {
			break
		}
	}

	// ToDo TMP
	if err == nil && attrs32Counters[b.payloadType.PayloadType().String()] == 0 {
		println(b.payloadType.PayloadType().String())
		arrow2.PrintRecord(record)
		attrs32Counters[b.payloadType.PayloadType().String()] += 1
	}

	return record, werror.Wrap(err)
}

// ToDo TMP
var attrs32Counters map[string]int = make(map[string]int)

func (b *Attrs32Builder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *Attrs32Builder) Schema() *arrow.Schema {
	return b.builder.Schema()
}

func (b *Attrs32Builder) PayloadType() *PayloadType {
	return b.payloadType
}

func (b *Attrs32Builder) Reset() {
	b.accumulator.Reset()
}

// Release releases the memory allocated by the builder.
func (b *Attrs32Builder) Release() {
	if !b.released {
		b.builder.Release()
		b.released = true
	}
}

func (b *Attrs32Builder) ShowSchema() {
	b.builder.ShowSchema()
}

// No sorting
// ==========

func UnsortedAttrs32() *Attrs32ByNothing {
	return &Attrs32ByNothing{}
}

func (s Attrs32ByNothing) Sort(attrs []Attr32) {
	// Do nothing
}

// Sorts the attributes by parentID, key, and value
// ================================================

func SortAttrs32ByParentIdKeyValue() *Attrs32ByParentIdKeyValue {
	return &Attrs32ByParentIdKeyValue{}
}

func (s Attrs32ByParentIdKeyValue) Sort(attrs []Attr32) {
	sort.Slice(attrs, func(i, j int) bool {
		attrsI := attrs[i]
		attrsJ := attrs[j]
		if attrsI.ParentID == attrsJ.ParentID {
			if attrsI.Key == attrsJ.Key {
				return IsLess(attrsI.Value, attrsJ.Value)
			} else {
				return attrsI.Key < attrsJ.Key
			}
		} else {
			return attrsI.ParentID < attrsJ.ParentID
		}
	})
}

// Sorts the attributes by key, parentID, and value
// ================================================

func SortAttrs32ByKeyParentIdValue() *Attrs32ByKeyParentIdValue {
	return &Attrs32ByKeyParentIdValue{}
}

func (s Attrs32ByKeyParentIdValue) Sort(attrs []Attr32) {
	sort.Slice(attrs, func(i, j int) bool {
		attrsI := attrs[i]
		attrsJ := attrs[j]
		if attrsI.Key == attrsJ.Key {
			if attrsI.ParentID == attrsJ.ParentID {
				return IsLess(attrsI.Value, attrsJ.Value)
			} else {
				return attrsI.ParentID < attrsJ.ParentID
			}
		} else {
			return attrsI.Key < attrsJ.Key
		}
	})
}

// Sorts the attributes by key, value, and parentID
// ================================================

func SortAttrs32ByKeyValueParentId() *Attrs32ByKeyValueParentId {
	return &Attrs32ByKeyValueParentId{}
}

func (s Attrs32ByKeyValueParentId) Sort(attrs []Attr32) {
	sort.Slice(attrs, func(i, j int) bool {
		attrsI := attrs[i]
		attrsJ := attrs[j]
		if attrsI.Key == attrsJ.Key {
			cmp := Compare(attrsI.Value, attrsJ.Value)
			if cmp == 0 {
				return attrsI.ParentID < attrsJ.ParentID
			} else {
				return cmp < 0
			}
		} else {
			return attrsI.Key < attrsJ.Key
		}
	})
}
