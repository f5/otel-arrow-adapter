package otlp

import (
	"github.com/apache/arrow/go/v10/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrow_utils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

type ResourceIds struct {
	Id                     int
	Attributes             *AttributeIds
	DroppedAttributesCount int
}

func NewResourceIds(resSpansDT *arrow.StructType) (*ResourceIds, error) {
	resId, resDT, err := arrow_utils.StructFieldIdFromStruct(resSpansDT, constants.RESOURCE)
	if err != nil {
		return nil, err
	}

	attributeIds, err := NewAttributeIds(resDT)
	if err != nil {
		return nil, err
	}

	droppedAttributesCount, _, err := arrow_utils.FieldIdFromStruct(resDT, constants.DROPPED_ATTRIBUTES_COUNT)
	if err != nil {
		return nil, err
	}

	return &ResourceIds{
		Id:                     resId,
		Attributes:             attributeIds,
		DroppedAttributesCount: droppedAttributesCount,
	}, nil
}

// NewResourceFrom creates a new Resource from the given array and row.
func NewResourceFrom(resList *arrow_utils.ListOfStructs, row int) (pcommon.Resource, error) {
	r := pcommon.NewResource()
	resDt, resArr, err := resList.StructArray(constants.RESOURCE, row)
	if err != nil {
		return r, err
	}

	// Read dropped attributes count
	droppedAttributesCount, err := arrow_utils.U32FromStructOld(resDt, resArr, row, constants.DROPPED_ATTRIBUTES_COUNT)
	if err != nil {
		return r, err
	}
	r.SetDroppedAttributesCount(droppedAttributesCount)

	// Read attributes
	attrs, err := arrow_utils.ListOfStructsFromStruct(resDt, resArr, row, constants.ATTRIBUTES)
	if err != nil {
		return r, err
	}
	if attrs != nil {
		err = attrs.CopyAttributesFrom(r.Attributes())
	}

	return r, err
}

// AppendResourceInto appends a resource into a given resource spans from an Arrow list of structs.
func AppendResourceInto(resSpans ptrace.ResourceSpans, resList *arrow_utils.ListOfStructs, row int, resIds *ResourceIds) error {
	r := resSpans.Resource()
	_, resArr, err := resList.StructById(resIds.Id, row)
	if err != nil {
		return err
	}

	// Read dropped attributes count
	droppedAttributesCount, err := arrow_utils.U32FromStruct(resArr, row, resIds.DroppedAttributesCount)
	if err != nil {
		return err
	}
	r.SetDroppedAttributesCount(droppedAttributesCount)

	// Read attributes
	err = AppendAttributesInto(r.Attributes(), resArr, row, resIds.Attributes)
	if err != nil {
		return err
	}

	return err
}
