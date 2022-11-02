package otlp

import (
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrow_utils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/otlp"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

type EventIds struct {
	Id                     int
	TimeUnixNano           int
	Name                   int
	Attributes             *otlp.AttributeIds
	DroppedAttributesCount int
}

func NewEventIds(spansDT *arrow.StructType) (*EventIds, error) {
	id, eventDT, err := arrow_utils.ListOfStructsFieldIdFromStruct(spansDT, constants.SPAN_EVENTS)
	if err != nil {
		return nil, err
	}

	timeUnixNanoId, timeUnixNanoFound := eventDT.FieldIdx(constants.TIME_UNIX_NANO)
	if !timeUnixNanoFound {
		return nil, fmt.Errorf("field %s not found", constants.TIME_UNIX_NANO)
	}
	nameId, nameFound := eventDT.FieldIdx(constants.NAME)
	if !nameFound {
		return nil, fmt.Errorf("field %s not found", constants.NAME)
	}
	droppedAttributesCountId, droppedAttributesCountFound := eventDT.FieldIdx(constants.DROPPED_ATTRIBUTES_COUNT)
	if !droppedAttributesCountFound {
		return nil, fmt.Errorf("field %s not found", constants.DROPPED_ATTRIBUTES_COUNT)
	}
	attributesId, err := otlp.NewAttributeIds(eventDT)
	if err != nil {
		return nil, err
	}

	return &EventIds{
		Id:                     id,
		TimeUnixNano:           timeUnixNanoId,
		Name:                   nameId,
		Attributes:             attributesId,
		DroppedAttributesCount: droppedAttributesCountId,
	}, nil
}

// AppendEventsInto initializes a Span's Events from an Arrow representation.
func AppendEventsInto(spans ptrace.SpanEventSlice, arrowSpans *arrow_utils.ListOfStructs, spanIdx int, ids *EventIds) error {
	events, err := arrowSpans.ListOfStructsById(spanIdx, ids.Id)
	if err != nil {
		return err
	}
	if events == nil {
		// No event found
		return nil
	}

	for eventIdx := events.Start(); eventIdx < events.End(); eventIdx++ {
		event := spans.AppendEmpty()

		if events.IsNull(eventIdx) {
			continue
		}

		timeUnixNano, err := events.U64FieldById(ids.TimeUnixNano, eventIdx)
		if err != nil {
			return err
		}
		event.SetTimestamp(pcommon.Timestamp(timeUnixNano))
		name, err := events.StringFieldById(ids.Name, eventIdx)
		if err != nil {
			return err
		}
		event.SetName(name)
		if err = otlp.AppendAttributesInto(event.Attributes(), events.Array(), eventIdx, ids.Attributes); err != nil {
			return err
		}
		dac, err := events.U32FieldById(ids.DroppedAttributesCount, eventIdx)
		if err != nil {
			return err
		}
		event.SetDroppedAttributesCount(dac)
	}
	return nil
}
