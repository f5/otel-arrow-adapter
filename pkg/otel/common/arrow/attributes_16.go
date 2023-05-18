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

// Attributes record builder for 16-bit Parent IDs.

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

// Schema is the Arrow schema for the OTLP Arrow Traces record.
var (
	AttrsSchema16 = arrow.NewSchema([]arrow.Field{
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint16},
		{Name: constants.AttrsRecordKey, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttrsRecordValue, Type: AnyValueDT},
	}, nil)
)

type (
	Attrs16Builder struct {
		released bool

		builder *builder.RecordBuilderExt // Record builder

		pib *builder.Uint16Builder
		kb  *builder.StringBuilder
		ab  *AnyValueBuilder

		accumulator *Attributes16Accumulator
		payloadType *PayloadType

		parentIdEncoding int
	}

	Attrs16ByNothing          struct{}
	Attrs16ByParentIdKeyValue struct{}
	Attrs16ByKeyParentIdValue struct{}
	Attrs16ByKeyValueParentId struct{}
)

func NewAttrs16Builder(rBuilder *builder.RecordBuilderExt, payloadType *PayloadType, sorter Attrs16Sorter) *Attrs16Builder {
	b := &Attrs16Builder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewAttributes16Accumulator(sorter),
		payloadType: payloadType,
	}
	b.init()
	return b
}

func NewAttrs16BuilderWithEncoding(rBuilder *builder.RecordBuilderExt, payloadType *PayloadType, config *Attrs16Config) *Attrs16Builder {
	b := &Attrs16Builder{
		released:         false,
		builder:          rBuilder,
		accumulator:      NewAttributes16Accumulator(config.Sorter),
		payloadType:      payloadType,
		parentIdEncoding: config.ParentIdEncoding,
	}

	b.init()
	return b
}

func (b *Attrs16Builder) init() {
	b.pib = b.builder.Uint16Builder(constants.ParentID)
	b.kb = b.builder.StringBuilder(constants.AttrsRecordKey)
	b.ab = AnyValueBuilderFrom(b.builder.SparseUnionBuilder(constants.AttrsRecordValue))
}

func (b *Attrs16Builder) Accumulator() *Attributes16Accumulator {
	return b.accumulator
}

func (b *Attrs16Builder) TryBuild() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(ErrBuilderAlreadyReleased)
	}

	prevParentID := uint16(0)
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

func (b *Attrs16Builder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *Attrs16Builder) Build() (arrow.Record, error) {
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
	if err == nil && countAttrs16[b.payloadType.PayloadType().String()] == 0 {
		println(b.payloadType.PayloadType().String())
		arrow2.PrintRecord(record)
		countAttrs16[b.payloadType.PayloadType().String()] += 1
	}

	return record, werror.Wrap(err)
}

var countAttrs16 map[string]int = make(map[string]int)

func (b *Attrs16Builder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *Attrs16Builder) Schema() *arrow.Schema {
	return b.builder.Schema()
}

func (b *Attrs16Builder) PayloadType() *PayloadType {
	return b.payloadType
}

func (b *Attrs16Builder) Reset() {
	b.accumulator.Reset()
}

// Release releases the memory allocated by the builder.
func (b *Attrs16Builder) Release() {
	if !b.released {
		b.builder.Release()
		b.released = true
	}
}

func (b *Attrs16Builder) ShowSchema() {
	b.builder.ShowSchema()
}

// Sorts the attributes by parentID, key, and value
// ================================================

func SortByParentIdKeyValueAttr16() *Attrs16ByParentIdKeyValue {
	return &Attrs16ByParentIdKeyValue{}
}

func (s Attrs16ByParentIdKeyValue) Sort(attrs []Attr16) {
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

// No sorting
// ==========

func UnsortedAttrs16() *Attrs16ByNothing {
	return &Attrs16ByNothing{}
}

func (s Attrs16ByNothing) Sort(attrs []Attr16) {
	// Do nothing
}

// Sorts the attributes by key, parentID, and value
// ================================================

func SortAttrs16ByKeyParentIdValue() *Attrs16ByKeyParentIdValue {
	return &Attrs16ByKeyParentIdValue{}
}

func (s Attrs16ByKeyParentIdValue) Sort(attrs []Attr16) {
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

func SortAttrs16ByKeyValueParentId() *Attrs16ByKeyValueParentId {
	return &Attrs16ByKeyValueParentId{}
}

func (s Attrs16ByKeyValueParentId) Sort(attrs []Attr16) {
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
