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

	aggrTempId, _ := arrowutils.FieldIDFromStruct(parentDT, constants.AggregationTemporality)

	return &UnivariateEHistogramIds{
		DataPoints:             dataPoints,
		AggregationTemporality: aggrTempId,
	}, nil
}

func UpdateUnivariateEHistogramFrom(ehistogram pmetric.ExponentialHistogram, arr *array.Struct, row int, ids *UnivariateEHistogramIds, smdata *SharedData, mdata *SharedData) error {
	if ids.AggregationTemporality >= 0 {
		value, err := arrowutils.I32FromArray(arr.Field(ids.AggregationTemporality), row)
		if err != nil {
			return err
		}
		ehistogram.SetAggregationTemporality(pmetric.AggregationTemporality(value))
	}

	los, err := arrowutils.ListOfStructsFromStruct(arr, ids.DataPoints.Id, row)
	if err != nil {
		return err
	}
	return AppendUnivariateEHistogramDataPointInto(ehistogram.DataPoints(), los, ids.DataPoints, smdata, mdata)
}
