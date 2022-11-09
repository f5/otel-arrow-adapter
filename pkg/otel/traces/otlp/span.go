package otlp

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrow_utils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/otlp"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

type SpansIds struct {
	Id                  int
	TraceId             int
	SpanId              int
	TraceState          int
	ParentSpanId        int
	Name                int
	Kind                int
	StartTimeUnixNano   int
	EndTimeUnixNano     int
	Attributes          *otlp.AttributeIds
	DropAttributesCount int
	Events              *EventIds
	DropEventsCount     int
	Links               *LinkIds
	DropLinksCount      int
	Status              *StatusIds
}

type StatusIds struct {
	Id      int
	Code    int
	Message int
}

func NewSpansIds(scopeSpansDT *arrow.StructType) (*SpansIds, error) {
	id, spanDT, err := arrow_utils.ListOfStructsFieldIdFromStruct(scopeSpansDT, constants.SPANS)
	if err != nil {
		return nil, err
	}

	traceId, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.TRACE_ID)
	if err != nil {
		return nil, err
	}

	spanId, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.SPAN_ID)
	if err != nil {
		return nil, err
	}

	traceState, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.TRACE_STATE)
	if err != nil {
		return nil, err
	}

	parentSpanId, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.PARENT_SPAN_ID)
	if err != nil {
		return nil, err
	}

	name, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.NAME)
	if err != nil {
		return nil, err
	}

	kind, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.KIND)
	if err != nil {
		return nil, err
	}

	startTimeUnixNano, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.START_TIME_UNIX_NANO)
	if err != nil {
		return nil, err
	}

	endTimeUnixNano, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.END_TIME_UNIX_NANO)
	if err != nil {
		return nil, err
	}

	attributes, err := otlp.NewAttributeIds(spanDT)
	if err != nil {
		return nil, err
	}

	droppedAttributesCount, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.DROPPED_ATTRIBUTES_COUNT)
	if err != nil {
		return nil, err
	}

	events, err := NewEventIds(spanDT)
	if err != nil {
		return nil, err
	}

	droppedEventsCount, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.DROPPED_EVENTS_COUNT)
	if err != nil {
		return nil, err
	}

	links, err := NewLinkIds(spanDT)
	if err != nil {
		return nil, err
	}

	droppedLinksCount, _, err := arrow_utils.FieldIdFromStruct(spanDT, constants.DROPPED_LINKS_COUNT)
	if err != nil {
		return nil, err
	}

	status, err := NewStatusIds(spanDT)
	if err != nil {
		return nil, err
	}

	return &SpansIds{
		Id:                  id,
		TraceId:             traceId,
		SpanId:              spanId,
		TraceState:          traceState,
		ParentSpanId:        parentSpanId,
		Name:                name,
		Kind:                kind,
		StartTimeUnixNano:   startTimeUnixNano,
		EndTimeUnixNano:     endTimeUnixNano,
		Attributes:          attributes,
		DropAttributesCount: droppedAttributesCount,
		Events:              events,
		DropEventsCount:     droppedEventsCount,
		Links:               links,
		DropLinksCount:      droppedLinksCount,
		Status:              status,
	}, nil
}

func NewStatusIds(spansDT *arrow.StructType) (*StatusIds, error) {
	statusId, statusDT, err := arrow_utils.StructFieldIdFromStruct(spansDT, constants.STATUS)
	if err != nil {
		return nil, err
	}

	code, _, err := arrow_utils.FieldIdFromStruct(statusDT, constants.STATUS_CODE)
	if err != nil {
		return nil, err
	}

	message, _, err := arrow_utils.FieldIdFromStruct(statusDT, constants.STATUS_MESSAGE)
	if err != nil {
		return nil, err
	}

	return &StatusIds{
		Id:      statusId,
		Code:    code,
		Message: message,
	}, nil
}

func AppendSpanInto(spans ptrace.SpanSlice, los *arrow_utils.ListOfStructs, row int, ids *SpansIds) error {
	span := spans.AppendEmpty()
	traceId, err := los.FixedSizeBinaryFieldById(ids.TraceId, row)
	if err != nil {
		return err
	}
	if len(traceId) != 16 {
		return fmt.Errorf("trace_id field should be 16 bytes")
	}
	spanId, err := los.FixedSizeBinaryFieldById(ids.SpanId, row)
	if err != nil {
		return err
	}
	if len(spanId) != 8 {
		return fmt.Errorf("span_id field should be 8 bytes")
	}
	traceState, err := los.StringFieldById(ids.TraceState, row)
	if err != nil {
		return err
	}
	parentSpanId, err := los.FixedSizeBinaryFieldById(ids.ParentSpanId, row)
	if err != nil {
		return err
	}
	if parentSpanId != nil && len(parentSpanId) != 8 {
		return fmt.Errorf("parent_span_id field should be 8 bytes")
	}
	name, err := los.StringFieldById(ids.Name, row)
	if err != nil {
		return err
	}
	kind, err := los.I32FieldById(ids.Kind, row)
	if err != nil {
		return err
	}
	startTimeUnixNano, err := los.U64FieldById(ids.StartTimeUnixNano, row)
	if err != nil {
		return err
	}
	endTimeUnixNano, err := los.U64FieldById(ids.EndTimeUnixNano, row)
	if err != nil {
		return err
	}
	droppedAttributesCount, err := los.U32FieldById(ids.DropAttributesCount, row)
	if err != nil {
		return err
	}
	droppedEventsCount, err := los.U32FieldById(ids.DropEventsCount, row)
	if err != nil {
		return err
	}
	droppedLinksCount, err := los.U32FieldById(ids.DropLinksCount, row)
	if err != nil {
		return err
	}
	statusDt, statusArr, err := los.StructById(ids.Status.Id, row)
	if err != nil {
		return err
	}
	if statusDt != nil {
		// Status exists
		message, err := arrow_utils.StringFromStruct(statusArr, row, ids.Status.Message)
		if err != nil {
			return err
		}
		span.Status().SetMessage(message)

		code, err := arrow_utils.I32FromStruct(statusArr, row, ids.Status.Code)
		if err != nil {
			return err
		}
		span.Status().SetCode(ptrace.StatusCode(code))
	}
	err = otlp.AppendAttributesInto(span.Attributes(), los.Array(), row, ids.Attributes)
	if err != nil {
		return err
	}

	if err := AppendEventsInto(span.Events(), los, row, ids.Events); err != nil {
		return err
	}
	if err := AppendLinksInto(span.Links(), los, row, ids.Links); err != nil {
		return err
	}
	var tid pcommon.TraceID
	var sid pcommon.SpanID
	var psid pcommon.SpanID
	copy(tid[:], traceId)
	copy(sid[:], spanId)
	copy(psid[:], parentSpanId)

	span.SetTraceID(tid)
	span.SetSpanID(sid)
	span.TraceState().FromRaw(traceState)
	span.SetParentSpanID(psid)
	span.SetName(name)
	span.SetKind(ptrace.SpanKind(kind))
	span.SetStartTimestamp(pcommon.Timestamp(startTimeUnixNano))
	span.SetEndTimestamp(pcommon.Timestamp(endTimeUnixNano))
	span.SetDroppedAttributesCount(droppedAttributesCount)
	span.SetDroppedEventsCount(droppedEventsCount)
	span.SetDroppedLinksCount(droppedLinksCount)
	return nil
}
