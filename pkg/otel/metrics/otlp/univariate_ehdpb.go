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

	arrowutils "github.com/f5/otel-arrow-adapter/pkg/arrow2"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

type EHistogramDataPointBucketsIds struct {
	Id           int
	Offset       int
	BucketCounts int
}

func NewEHistogramDataPointBucketsIds(parentId int, parentDT *arrow.StructType) (*EHistogramDataPointBucketsIds, error) {
	offset, _ := arrowutils.FieldIDFromStruct(parentDT, constants.ExpHistogramOffset)
	bucketCounts, _ := arrowutils.FieldIDFromStruct(parentDT, constants.ExpHistogramBucketCounts)

	return &EHistogramDataPointBucketsIds{
		Id:           parentId,
		Offset:       offset,
		BucketCounts: bucketCounts,
	}, nil
}

func AppendUnivariateEHistogramDataPointBucketsInto(dpBuckets pmetric.ExponentialHistogramDataPointBuckets, ehdp *array.Struct, ids *EHistogramDataPointBucketsIds, row int) error {
	if ehdp == nil {
		return nil
	}

	offsetArr := ehdp.Field(ids.Offset)
	if offsetArr != nil {
		if i32OffsetArr, ok := offsetArr.(*array.Int32); ok {
			dpBuckets.SetOffset(i32OffsetArr.Value(row))
		} else {
			return fmt.Errorf("field %q is not an int32", constants.ExpHistogramOffset)
		}
	}

	bucketCountsArr := ehdp.Field(ids.BucketCounts)
	if bucketCountsArr != nil {
		if i64BucketCountsArr, ok := bucketCountsArr.(*array.List); ok {
			start := int(i64BucketCountsArr.Offsets()[row])
			end := int(i64BucketCountsArr.Offsets()[row+1])
			values := i64BucketCountsArr.ListValues()

			if v, ok := values.(*array.Uint64); ok {
				dpbs := dpBuckets.BucketCounts()
				dpbs.EnsureCapacity(end - start)
				for i := start; i < end; i++ {
					dpbs.Append(v.Value(i))
				}
			}
		} else {
			return fmt.Errorf("field %q is not an int64", constants.ExpHistogramBucketCounts)
		}
	}

	return nil
}
