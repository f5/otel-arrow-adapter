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

	"github.com/apache/arrow/go/v12/arrow"

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

		ib *builder.Uint16Builder
		kb *builder.StringBuilder
		ab *AnyValueBuilder

		accumulator *Attributes16Accumulator
		payloadType *PayloadType
	}
)

func NewAttrs16Builder(rBuilder *builder.RecordBuilderExt, payloadType *PayloadType) *Attrs16Builder {
	b := &Attrs16Builder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewAttributes16Accumulator(),
		payloadType: payloadType,
	}
	b.init()
	return b
}

func (b *Attrs16Builder) init() {
	b.ib = b.builder.Uint16Builder(constants.ParentID)
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

	for _, attr := range b.accumulator.SortedAttrs() {
		b.ib.Append(attr.ParentID)
		b.kb.Append(attr.Key)
		if err := b.ab.Append(attr.Value); err != nil {
			return nil, werror.Wrap(err)
		}
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	} else {
		//PrintRecord(record)
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
	return record, werror.Wrap(err)
}

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
