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

package arrow

import (
	"errors"
	"math"
	"sort"

	"github.com/apache/arrow/go/v12/arrow"
	"go.opentelemetry.io/collector/pdata/pmetric"

	carrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

var (
	// HistogramDataPointSchema is the Arrow Schema describing a histogram
	// data point.
	// Related record.
	HistogramDataPointSchema = arrow.NewSchema([]arrow.Field{
		// Unique identifier of the NDP. This ID is used to identify the
		// relationship between the NDP, its attributes and exemplars.
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding)},
		// The ID of the parent metric.
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint16},
		{Name: constants.StartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.HistogramCount, Type: arrow.PrimitiveTypes.Uint64, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.HistogramSum, Type: arrow.PrimitiveTypes.Float64, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.HistogramBucketCounts, Type: arrow.ListOf(arrow.PrimitiveTypes.Uint64), Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.HistogramExplicitBounds, Type: arrow.ListOf(arrow.PrimitiveTypes.Float64), Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Exemplars, Type: arrow.ListOf(ExemplarDT), Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Flags, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.HistogramMin, Type: arrow.PrimitiveTypes.Float64, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.HistogramMax, Type: arrow.PrimitiveTypes.Float64, Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

type (
	// HistogramDataPointBuilder is a builder for histogram data points.
	HistogramDataPointBuilder struct {
		released bool

		builder *builder.RecordBuilderExt

		ib  *builder.Uint32DeltaBuilder // id builder
		pib *builder.Uint16Builder      // parent_id builder

		stunb *builder.TimestampBuilder // start_time_unix_nano builder
		tunb  *builder.TimestampBuilder // time_unix_nano builder
		hcb   *builder.Uint64Builder    // histogram_count builder
		hsb   *builder.Float64Builder   // histogram_sum builder
		hbclb *builder.ListBuilder      // histogram_bucket_counts list builder
		hbcb  *builder.Uint64Builder    // histogram_bucket_counts builder
		heblb *builder.ListBuilder      // histogram_explicit_bounds list builder
		hebb  *builder.Float64Builder   // histogram_explicit_bounds builder
		elb   *builder.ListBuilder      // exemplars builder
		eb    *ExemplarBuilder          // exemplar builder
		fb    *builder.Uint32Builder    // flags builder
		hmib  *builder.Float64Builder   // histogram_min builder
		hmab  *builder.Float64Builder   // histogram_max builder

		accumulator *HDPAccumulator
		attrsAccu   *carrow.Attributes32Accumulator
	}

	HDP struct {
		ParentID uint16
		hdp      *pmetric.HistogramDataPoint
	}

	HDPAccumulator struct {
		groupCount uint32
		hdps       []HDP
	}
)

// NewHistogramDataPointBuilder creates a new HistogramDataPointBuilder.
func NewHistogramDataPointBuilder(rBuilder *builder.RecordBuilderExt) *HistogramDataPointBuilder {
	b := &HistogramDataPointBuilder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewHDPAccumulator(),
	}

	b.init()
	return b
}

func (b *HistogramDataPointBuilder) init() {
	b.ib = b.builder.Uint32DeltaBuilder(constants.ID)
	b.ib.SetMaxDelta(1)
	b.pib = b.builder.Uint16Builder(constants.ParentID)

	b.stunb = b.builder.TimestampBuilder(constants.StartTimeUnixNano)
	b.tunb = b.builder.TimestampBuilder(constants.TimeUnixNano)
	b.hcb = b.builder.Uint64Builder(constants.HistogramCount)
	b.hsb = b.builder.Float64Builder(constants.HistogramSum)
	b.hbclb = b.builder.ListBuilder(constants.HistogramBucketCounts)
	b.heblb = b.builder.ListBuilder(constants.HistogramExplicitBounds)
	b.elb = b.builder.ListBuilder(constants.Exemplars)
	b.hbcb = b.hbclb.Uint64Builder()
	b.hebb = b.heblb.Float64Builder()
	b.eb = ExemplarBuilderFrom(b.elb.StructBuilder())
	b.fb = b.builder.Uint32Builder(constants.Flags)
	b.hmib = b.builder.Float64Builder(constants.HistogramMin)
	b.hmab = b.builder.Float64Builder(constants.HistogramMax)
}

func (b *HistogramDataPointBuilder) SetAttributesAccumulator(accu *carrow.Attributes32Accumulator) {
	b.attrsAccu = accu
}

func (b *HistogramDataPointBuilder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *HistogramDataPointBuilder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *HistogramDataPointBuilder) Accumulator() *HDPAccumulator {
	return b.accumulator
}

// Build builds the underlying array.
//
// Once the array is no longer needed, Release() should be called to free the memory.
func (b *HistogramDataPointBuilder) Build() (record arrow.Record, err error) {
	schemaNotUpToDateCount := 0

	// Loop until the record is built successfully.
	// Intermediaries steps may be required to update the schema.
	for {
		b.attrsAccu.Reset()
		record, err = b.TryBuild(b.attrsAccu)
		if err != nil {
			if record != nil {
				record.Release()
			}

			switch {
			case errors.Is(err, schema.ErrSchemaNotUpToDate):
				schemaNotUpToDateCount++
				if schemaNotUpToDateCount > 5 {
					panic("Too many consecutive schema updates. This shouldn't happen.")
				}
			default:
				return nil, werror.Wrap(err)
			}
		} else {
			break
		}
	}
	return record, werror.Wrap(err)
}

func (b *HistogramDataPointBuilder) Reset() {
	b.accumulator.Reset()
}

func (b *HistogramDataPointBuilder) PayloadType() *carrow.PayloadType {
	return carrow.PayloadTypes.Histogram
}

// Release releases the underlying memory.
func (b *HistogramDataPointBuilder) Release() {
	if b.released {
		return
	}

	b.released = true
	b.builder.Release()
}

func (b *HistogramDataPointBuilder) TryBuild(attrsAccu *carrow.Attributes32Accumulator) (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(carrow.ErrBuilderAlreadyReleased)
	}

	b.accumulator.Sort()

	for ID, hdpRec := range b.accumulator.hdps {
		hdp := hdpRec.hdp
		b.ib.Append(uint32(ID))
		b.pib.Append(hdpRec.ParentID)

		// Attributes
		err = attrsAccu.AppendUniqueAttributesWithID(uint32(ID), hdp.Attributes(), nil, nil)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		b.stunb.Append(arrow.Timestamp(hdp.StartTimestamp()))
		b.tunb.Append(arrow.Timestamp(hdp.Timestamp()))

		b.hcb.Append(hdp.Count())
		if hdp.HasSum() {
			b.hsb.AppendNonZero(hdp.Sum())
		} else {
			b.hsb.AppendNull()
		}

		hbc := hdp.BucketCounts()
		hbcc := hbc.Len()
		if err := b.hbclb.Append(hbcc, func() error {
			for i := 0; i < hbcc; i++ {
				b.hbcb.Append(hbc.At(i))
			}
			return nil
		}); err != nil {
			return nil, werror.Wrap(err)
		}

		heb := hdp.ExplicitBounds()
		hebc := heb.Len()
		if err := b.heblb.Append(hebc, func() error {
			for i := 0; i < hebc; i++ {
				b.hebb.AppendNonZero(heb.At(i))
			}
			return nil
		}); err != nil {
			return nil, werror.Wrap(err)
		}

		exs := hdp.Exemplars()
		ec := exs.Len()
		if err := b.elb.Append(ec, func() error {
			for i := 0; i < ec; i++ {
				if err := b.eb.Append(exs.At(i)); err != nil {
					return werror.Wrap(err)
				}
			}
			return nil
		}); err != nil {
			return nil, werror.Wrap(err)
		}
		b.fb.Append(uint32(hdp.Flags()))

		if hdp.HasMin() {
			b.hmib.AppendNonZero(hdp.Min())
		} else {
			b.hmib.AppendNull()
		}
		if hdp.HasMax() {
			b.hmab.AppendNonZero(hdp.Max())
		} else {
			b.hmab.AppendNull()
		}
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}
	return
}

func NewHDPAccumulator() *HDPAccumulator {
	return &HDPAccumulator{
		groupCount: 0,
		hdps:       make([]HDP, 0),
	}
}

func (a *HDPAccumulator) IsEmpty() bool {
	return len(a.hdps) == 0
}

func (a *HDPAccumulator) Append(
	metricID uint16,
	hdps pmetric.HistogramDataPointSlice,
) error {
	if a.groupCount == math.MaxUint32 {
		panic("The maximum number of group of histogram data points has been reached (max is uint32).")
	}

	if hdps.Len() == 0 {
		return nil
	}

	for i := 0; i < hdps.Len(); i++ {
		hdp := hdps.At(i)

		a.hdps = append(a.hdps, HDP{
			ParentID: metricID,
			hdp:      &hdp,
		})
	}

	a.groupCount++

	return nil
}

func (a *HDPAccumulator) Sort() {
	sort.Slice(a.hdps, func(i, j int) bool {
		return a.hdps[i].hdp.StartTimestamp() < a.hdps[j].hdp.StartTimestamp()
	})
}

func (a *HDPAccumulator) Reset() {
	a.groupCount = 0
	a.hdps = a.hdps[:0]
}
