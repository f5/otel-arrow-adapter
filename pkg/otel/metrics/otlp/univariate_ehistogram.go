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
)

type UnivariateEHistogramIds struct {
	DataPoints             *UnivariateEHistogramDataPointIds
	AggregationTemporality int
}

func NewUnivariateEHistogramIds(parentDT *arrow.StructType) (*UnivariateEHistogramIds, error) {
	dataPoints, err := NewUnivariateEHistogramDataPointIds(parentDT)
	if err != nil {
		return nil, err
	}

	aggrTempId, found := parentDT.FieldIdx(constants.AggregationTemporality)
	if !found {
		return nil, fmt.Errorf("missing field %q", constants.AggregationTemporality)
	}

	return &UnivariateEHistogramIds{
		DataPoints:             dataPoints,
		AggregationTemporality: aggrTempId,
	}, nil
}

func UpdateUnivariateEHistogramFrom(ehistogram pmetric.ExponentialHistogram, arr *array.Struct, row int, ids *UnivariateEHistogramIds, smdata *SharedData, mdata *SharedData) error {
	atArr, ok := arr.Field(ids.AggregationTemporality).(*array.Int32)
	if !ok {
		return fmt.Errorf("field %q is not an int64", constants.AggregationTemporality)
	}
	ehistogram.SetAggregationTemporality(pmetric.AggregationTemporality(atArr.Value(row)))

	los, err := arrowutils.ListOfStructsFromStruct(arr, ids.DataPoints.Id, row)
	if err != nil {
		return err
	}
	return AppendUnivariateEHistogramDataPointInto(ehistogram.DataPoints(), los, ids.DataPoints, smdata, mdata)
}
