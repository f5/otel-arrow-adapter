package otlp

import (
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"

	arrow_utils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	common_arrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

type AttributeIds struct {
	Id int
}

func NewAttributeIds(structDT *arrow.StructType) (*AttributeIds, error) {
	id, found := structDT.FieldIdx(constants.ATTRIBUTES)
	if !found {
		return nil, fmt.Errorf("`attributes` field not found in Arrow struct")
	}
	return &AttributeIds{Id: id}, nil
}

func AppendAttributesInto(attrs pcommon.Map, parentArr *array.Struct, row int, attributeIds *AttributeIds) error {
	marr, start, end, err := attributesFromStruct(attributeIds.Id, parentArr, row)
	if err != nil {
		return err
	}
	if marr == nil {
		return nil
	}
	attrs.EnsureCapacity(end - start)

	keys := marr.Keys()
	values := marr.Items().(*array.SparseUnion)

	for i := start; i < end; i++ {
		key, err := arrow_utils.StringFromArray(keys, i)
		if err != nil {
			return err
		}
		tcode := int8(values.ChildID(i))
		switch tcode {
		case common_arrow.StrCode:
			val, err := arrow_utils.StringFromArray(values.Field(int(tcode)), i)
			if err != nil {
				return err
			}
			attrs.PutStr(key, val)
		case common_arrow.I64Code:
			val := values.Field(int(tcode)).(*array.Int64).Value(i)
			attrs.PutInt(key, val)
		case common_arrow.F64Code:
			val := values.Field(int(tcode)).(*array.Float64).Value(i)
			attrs.PutDouble(key, val)
		case common_arrow.BoolCode:
			val := values.Field(int(tcode)).(*array.Boolean).Value(i)
			attrs.PutBool(key, val)
		case common_arrow.BinaryCode:
			val, err := arrow_utils.BinaryFromArray(values.Field(int(tcode)), i)
			if err != nil {
				return err
			}
			attrs.PutEmptyBytes(key).Append(val...)
		default:
			return fmt.Errorf("unknow type code `%d` in attributes union array", tcode)
		}
	}
	return nil
}

func attributesFromStruct(fieldId int, parentArr *array.Struct, row int) (marr *array.Map, start int, end int, err error) {
	start = 0
	end = 0

	column := parentArr.Field(fieldId)
	switch arr := column.(type) {
	case *array.Map:
		if arr.IsNull(row) {
			return
		}

		start = int(arr.Offsets()[row])
		end = int(arr.Offsets()[row+1])
		marr = arr
	default:
		err = fmt.Errorf("`attributes` is not an Arrow map")
	}
	return
}
