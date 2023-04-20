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

import (
	"sort"

	"github.com/apache/arrow/go/v12/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// EventSchema is the Arrow Data Type describing an event (as a related record
// to the main trace record).
var (
	EventSchema = arrow.NewSchema([]arrow.Field{
		{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Name, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttributesID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.DroppedAttributesCount, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

type (
	// EventBuilder is an Arrow builder for events.
	EventBuilder struct {
		released bool

		builder *builder.RecordBuilderExt

		tunb *builder.TimestampBuilder   // `time_unix_nano` builder
		nb   *builder.StringBuilder      // `name` builder
		aib  *builder.Uint32DeltaBuilder // attributes id builder
		dacb *builder.Uint32Builder      // `dropped_attributes_count` builder

		accumulator *EventAccumulator
	}

	// Event is an internal representation of an event used by the
	// EventAccumulator.
	Event struct {
		ID                     uint32
		TimeUnixNano           pcommon.Timestamp
		Name                   string
		Attributes             pcommon.Map
		DroppedAttributesCount uint32
	}

	// EventAccumulator is an accumulator for events that is used to sort events
	// globally in order to improve compression.
	EventAccumulator struct {
		groupCount uint32
		events     []Event
	}
)

func NewEventBuilder(rBuilder *builder.RecordBuilderExt) (*EventBuilder, error) {
	b := &EventBuilder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewEventAccumulator(),
	}
	if err := b.init(); err != nil {
		return nil, werror.Wrap(err)
	}
	return b, nil
}

func (b *EventBuilder) init() error {
	b.tunb = b.builder.TimestampBuilder(constants.TimeUnixNano)
	b.nb = b.builder.StringBuilder(constants.Name)
	b.aib = b.builder.Uint32DeltaBuilder(constants.AttributesID)
	b.dacb = b.builder.Uint32Builder(constants.DroppedAttributesCount)
	return nil
}

func (b *EventBuilder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *EventBuilder) Accumulator() *EventAccumulator {
	return b.accumulator
}

func (b *EventBuilder) Build(attrsAccu *acommon.AttributesAccumulator) (arrow.Record, error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	b.accumulator.Sort()

	for _, event := range b.accumulator.events {
		b.tunb.Append(arrow.Timestamp(event.TimeUnixNano.AsTime().UnixNano()))
		b.nb.AppendNonEmpty(event.Name)

		// Attributes
		ID, err := attrsAccu.Append(event.Attributes)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		if ID >= 0 {
			b.aib.Append(uint32(ID))
		} else {
			b.aib.AppendNull()
		}

		b.dacb.AppendNonZero(event.DroppedAttributesCount)
	}

	record, err := b.builder.NewRecord()
	if err != nil {
		initErr := b.init()
		if initErr != nil {
			return nil, werror.Wrap(initErr)
		}
	}
	return record, nil
}

// Release releases the memory allocated by the builder.
func (b *EventBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}

// NewEventAccumulator creates a new EventAccumulator.
func NewEventAccumulator() *EventAccumulator {
	return &EventAccumulator{
		groupCount: 0,
		events:     make([]Event, 0),
	}
}

// Append appends a slice of events to the accumulator.
func (a *EventAccumulator) Append(events ptrace.SpanEventSlice) (int64, error) {
	ID := a.groupCount

	if events.Len() == 0 {
		return -1, nil
	}

	for i := 0; i < events.Len(); i++ {
		evt := events.At(i)
		a.events = append(a.events, Event{
			ID:                     ID,
			TimeUnixNano:           evt.Timestamp(),
			Name:                   evt.Name(),
			Attributes:             evt.Attributes(),
			DroppedAttributesCount: evt.DroppedAttributesCount(),
		})
	}

	a.groupCount++

	return int64(ID), nil
}

func (a *EventAccumulator) Sort() {
	sort.Slice(a.events, func(i, j int) bool {
		if a.events[i].Name == a.events[j].Name {
			return a.events[i].TimeUnixNano < a.events[j].TimeUnixNano
		} else {
			return a.events[i].Name < a.events[j].Name
		}
	})
}

func (a *EventAccumulator) Reset() {
	a.groupCount = 0
	a.events = a.events[:0]
}
