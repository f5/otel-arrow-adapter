package otlp

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow/array"
	"go.opentelemetry.io/collector/pdata/pmetric"

	arrow_utils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/metrics/arrow"
)

func UpdateValueFromExemplar(v pmetric.Exemplar, vArr *array.DenseUnion, row int) error {
	tcode := int8(vArr.ChildID(row))
	switch tcode {
	case arrow.I64Code:
		val, err := arrow_utils.I64FromArray(vArr.Field(int(tcode)), row)
		if err != nil {
			return err
		}
		v.SetIntValue(val)
	case arrow.F64Code:
		val, err := arrow_utils.F64FromArray(vArr.Field(int(tcode)), row)
		if err != nil {
			return err
		}
		v.SetDoubleValue(val)
	default:
		return fmt.Errorf("UpdateValueFromExemplar: unknow type code `%d` in dense union array", tcode)
	}
	return nil
}

func UpdateValueFromNumberDataPoint(v pmetric.NumberDataPoint, vArr *array.DenseUnion, row int) error {
	tcode := int8(vArr.ChildID(row))
	switch tcode {
	case arrow.I64Code:
		val, err := arrow_utils.I64FromArray(vArr.Field(int(tcode)), row)
		if err != nil {
			return err
		}
		v.SetIntValue(val)
	case arrow.F64Code:
		val, err := arrow_utils.F64FromArray(vArr.Field(int(tcode)), row)
		if err != nil {
			return err
		}
		v.SetDoubleValue(val)
	default:
		return fmt.Errorf("UpdateValueFromNumberDataPoint: unknow type code `%d` in dense union array", tcode)
	}
	return nil
}
