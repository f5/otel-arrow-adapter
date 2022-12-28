// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlp

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"go.opentelemetry.io/collector/pdata/pmetric"

	arrowutils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	ametric "github.com/f5/otel-arrow-adapter/pkg/otel/metrics/arrow"
)

type UnivariateMetricIds struct {
	Id                      int
	UnivariateGaugeIds      *UnivariateGaugeIds
	UnivariateSumIds        *UnivariateSumIds
	UnivariateSummaryIds    *UnivariateSummaryIds
	UnivariateHistogramIds  *UnivariateHistogramIds
	UnivariateEHistogramIds *UnivariateEHistogramIds
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

	histogramDT, ok := dataDT.Fields()[ametric.HistogramCode].Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("histogram field is not a struct")
	}
	histogramIds, err := NewUnivariateHistogramIds(histogramDT)
	if err != nil {
		return nil, err
	}

	ehistogramDT, ok := dataDT.Fields()[ametric.ExpHistogramCode].Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("ehistogram field is not a struct")
	}
	ehistogramIds, err := NewUnivariateEHistogramIds(ehistogramDT)
	if err != nil {
		return nil, err
	}

	return &UnivariateMetricIds{
		Id:                      id,
		UnivariateGaugeIds:      gaugeIds,
		UnivariateSumIds:        sumIds,
		UnivariateSummaryIds:    summaryIds,
		UnivariateHistogramIds:  histogramIds,
		UnivariateEHistogramIds: ehistogramIds,
	}, nil
}

func UpdateUnivariateMetricFrom(metric pmetric.Metric, los *arrowutils.ListOfStructs, row int, ids *UnivariateMetricIds, smdata *SharedData, mdata *SharedData) error {
	arr, ok := los.FieldByID(ids.Id).(*array.SparseUnion)
	if !ok {
		return fmt.Errorf("field %q is not a sparse union", constants.DATA)
	}
	tcode := int8(arr.ChildID(row))
	switch tcode {
	case ametric.GaugeCode:
		return UpdateUnivariateGaugeFrom(metric.SetEmptyGauge(), arr.Field(int(tcode)).(*array.Struct), row, ids.UnivariateGaugeIds, smdata, mdata)
	case ametric.SumCode:
		return UpdateUnivariateSumFrom(metric.SetEmptySum(), arr.Field(int(tcode)).(*array.Struct), row, ids.UnivariateSumIds, smdata, mdata)
	case ametric.SummaryCode:
		return UpdateUnivariateSummaryFrom(metric.SetEmptySummary(), arr.Field(int(tcode)).(*array.Struct), row, ids.UnivariateSummaryIds, smdata, mdata)
	case ametric.HistogramCode:
		return UpdateUnivariateHistogramFrom(metric.SetEmptyHistogram(), arr.Field(int(tcode)).(*array.Struct), row, ids.UnivariateHistogramIds, smdata, mdata)
	case ametric.ExpHistogramCode:
		return UpdateUnivariateEHistogramFrom(metric.SetEmptyExponentialHistogram(), arr.Field(int(tcode)).(*array.Struct), row, ids.UnivariateEHistogramIds, smdata, mdata)
	default:
		return fmt.Errorf("UpdateUnivariateMetricFrom: unknown type code %d", tcode)
	}
}
