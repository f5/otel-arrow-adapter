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

package arrow2

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// EHistogramDataPointBucketsDT is the Arrow Data Type describing an exponential histogram data point buckets.
var (
	EHistogramDataPointBucketsDT = arrow.StructOf(
		arrow.Field{Name: constants.ExpHistogramOffset, Type: arrow.PrimitiveTypes.Int32, Metadata: schema.Metadata(schema.Optional)},
		arrow.Field{Name: constants.ExpHistogramBucketCounts, Type: arrow.ListOf(arrow.PrimitiveTypes.Uint64), Metadata: schema.Metadata(schema.Optional)},
	)
)

// EHistogramDataPointBucketsBuilder is a builder for exponential histogram data point buckets.
type EHistogramDataPointBucketsBuilder struct {
	released bool

	builder *builder.StructBuilder

	ob   *builder.Int32Builder  // offset builder
	bclb *builder.ListBuilder   // exp histogram bucket counts list builder
	bcb  *builder.Uint64Builder // exp histogram bucket counts builder
}

// EHistogramDataPointBucketsBuilderFrom creates a new EHistogramDataPointBucketsBuilder from an existing StructBuilder.
func EHistogramDataPointBucketsBuilderFrom(b *builder.StructBuilder) *EHistogramDataPointBucketsBuilder {
	bclb := b.ListBuilder(constants.ExpHistogramBucketCounts)
	return &EHistogramDataPointBucketsBuilder{
		released: false,
		builder:  b,

		ob:   b.Int32Builder(constants.ExpHistogramOffset),
		bclb: bclb,
		bcb:  bclb.Uint64Builder(),
	}
}

// Build builds the underlying array.
//
// Once the array is no longer needed, Release() should be called to free the memory.
func (b *EHistogramDataPointBucketsBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("EHistogramDataPointBucketsBuilder: Build() called after Release()")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the underlying memory.
func (b *EHistogramDataPointBucketsBuilder) Release() {
	if b.released {
		return
	}

	b.released = true
	b.builder.Release()
}

// Append appends a new histogram data point to the builder.
func (b *EHistogramDataPointBucketsBuilder) Append(hdpb pmetric.ExponentialHistogramDataPointBuckets) error {
	if b.released {
		return fmt.Errorf("EHistogramDataPointBucketsBuilder: Reserve() called after Release()")
	}

	return b.builder.Append(hdpb, func() error {
		b.ob.Append(hdpb.Offset())

		bc := hdpb.BucketCounts()
		bcc := bc.Len()
		return b.bclb.Append(bcc, func() error {
			for i := 0; i < bcc; i++ {
				b.bcb.Append(bc.At(i))
			}
			return nil
		})
	})
}