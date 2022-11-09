package otlp

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"go.opentelemetry.io/collector/pdata/pmetric"

	arrow_utils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	ametric "github.com/f5/otel-arrow-adapter/pkg/otel/metrics/arrow"
)

type UnivariateMetricIds struct {
	Id                   int
	UnivariateGaugeIds   *UnivariateGaugeIds
	UnivariateSumIds     *UnivariateSumIds
	UnivariateSummaryIds *UnivariateSummaryIds
}

func NewUnivariateMetricIds(parentDT *arrow.StructType) (*UnivariateMetricIds, error) {
	id, found := parentDT.FieldIdx(constants.DATA)
	if !found {
		return nil, fmt.Errorf("field %q not found in struct", constants.DATA)
	}
	dataDT, ok := parentDT.Field(id).Type.(*arrow.SparseUnionType)
	if !ok {
		return nil, fmt.Errorf("field %q is not a sparse union", constants.DATA)
	}

	gaugeDT, ok := dataDT.Fields()[ametric.GaugeCode].Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("gauge field is not a struct")
	}
	gaugeIds, err := NewUnivariateGaugeIds(gaugeDT)
	if err != nil {
		return nil, err
	}

	sumDT, ok := dataDT.Fields()[ametric.SumCode].Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("sum field is not a struct")
	}
	sumIds, err := NewUnivariateSumIds(sumDT)
	if err != nil {
		return nil, err
	}

	summaryDT, ok := dataDT.Fields()[ametric.SummaryCode].Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("summary field is not a struct")
	}
	summaryIds, err := NewUnivariateSummaryIds(summaryDT)
	if err != nil {
		return nil, err
	}

	println(sumDT)
	println(summaryDT)
	return &UnivariateMetricIds{
		Id:                   id,
		UnivariateGaugeIds:   gaugeIds,
		UnivariateSumIds:     sumIds,
		UnivariateSummaryIds: summaryIds,
	}, nil
}

func UpdateUnivariateMetricFrom(metric pmetric.Metric, los *arrow_utils.ListOfStructs, row int, ids *UnivariateMetricIds) error {
	arr, ok := los.FieldById(ids.Id).(*array.SparseUnion)
	if !ok {
		return fmt.Errorf("field %q is not a sparse union", constants.DATA)
	}
	tcode := int8(arr.ChildID(row))
	switch tcode {
	case ametric.GaugeCode:
		return UpdateUnivariateGaugeFrom(metric.SetEmptyGauge(), arr.Field(int(tcode)).(*array.Struct), row, ids.UnivariateGaugeIds)
	case ametric.SumCode:
		return UpdateUnivariateSumFrom(metric.SetEmptySum(), arr.Field(int(tcode)).(*array.Struct), row, ids.UnivariateSumIds)
	case ametric.SummaryCode:
		return UpdateUnivariateSummaryFrom(metric.SetEmptySummary(), arr.Field(int(tcode)).(*array.Struct), row, ids.UnivariateSummaryIds)
	default:
		return fmt.Errorf("UpdateUnivariateMetricFrom: unknown type code %d", tcode)
	}
}
