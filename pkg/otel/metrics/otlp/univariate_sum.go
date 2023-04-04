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
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"go.opentelemetry.io/collector/pdata/pmetric"

	arrowutils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

type UnivariateSumIds struct {
	DataPoints             *UnivariateNdpIds
	AggregationTemporality int
	IsMonotonic            int
}

func NewUnivariateSumIds(parentDT *arrow.StructType) (*UnivariateSumIds, error) {
	dataPoints, err := NewUnivariateNdpIds(parentDT)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	aggrTempId, _ := arrowutils.FieldIDFromStruct(parentDT, constants.AggregationTemporality)
	isMonotonicId, _ := arrowutils.FieldIDFromStruct(parentDT, constants.IsMonotonic)

	return &UnivariateSumIds{
		DataPoints:             dataPoints,
		AggregationTemporality: aggrTempId,
		IsMonotonic:            isMonotonicId,
	}, nil
}

func UpdateUnivariateSumFrom(sum pmetric.Sum, arr *array.Struct, row int, ids *UnivariateSumIds, smdata *SharedData, mdata *SharedData) error {
	if ids.AggregationTemporality >= 0 {
		value, err := arrowutils.I32FromArray(arr.Field(ids.AggregationTemporality), row)
		if err != nil {
			return werror.Wrap(err)
		}

		sum.SetAggregationTemporality(pmetric.AggregationTemporality(value))
	}

	if ids.IsMonotonic >= 0 {
		imArr, ok := arr.Field(ids.IsMonotonic).(*array.Boolean)
		if !ok {
			return werror.Wrap(ErrNotArrayBoolean)
		}
		sum.SetIsMonotonic(imArr.Value(row))
	}

	los, err := arrowutils.ListOfStructsFromStruct(arr, ids.DataPoints.Id, row)
	if err != nil {
		return werror.Wrap(err)
	}
	return AppendUnivariateNdpInto(sum.DataPoints(), los, ids.DataPoints, smdata, mdata)
}
