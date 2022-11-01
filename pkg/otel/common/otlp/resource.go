package otlp

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	arrow_utils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// NewResourceFrom creates a new Resource from the given array and row.
func NewResourceFrom(resList *arrow_utils.ListOfStructs, row int) (pcommon.Resource, error) {
	r := pcommon.NewResource()
	resDt, resArr, err := resList.StructArray(constants.RESOURCE, row)
	if err != nil {
		return r, err
	}

	// Read dropped attributes count
	droppedAttributesCount, err := arrow_utils.U32FromStruct(resDt, resArr, row, constants.DROPPED_ATTRIBUTES_COUNT)
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
