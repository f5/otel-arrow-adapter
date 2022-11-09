package otlp

import (
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"go.opentelemetry.io/collector/pdata/pmetric"

	arrow_utils "github.com/f5/otel-arrow-adapter/pkg/arrow"
)

type UnivariateGaugeIds struct {
	DataPoints *UnivariateNdpIds
}

func NewUnivariateGaugeIds(parentDT *arrow.StructType) (*UnivariateGaugeIds, error) {
	dataPoints, err := NewUnivariateNdpIds(parentDT)
	if err != nil {
		return nil, err
	}

	return &UnivariateGaugeIds{
		DataPoints: dataPoints,
	}, nil
}

func UpdateUnivariateGaugeFrom(gauge pmetric.Gauge, arr *array.Struct, row int, ids *UnivariateGaugeIds) error {
	los, err := arrow_utils.ListOfStructsFromStruct(arr, ids.DataPoints.Id, row)
	if err != nil {
		return err
	}
	return AppendUnivariateNdpInto(gauge.DataPoints(), los, ids.DataPoints)
}
