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
	"fmt"
	"math"
	"sort"

	"github.com/apache/arrow/go/v12/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

var (
	// NumberDataPointSchema is the Arrow schema representing number data points.
	// Related record.
	NumberDataPointSchema = arrow.NewSchema([]arrow.Field{
		// Unique identifier of the NDP. This ID is used to identify the
		// relationship between the NDP, its attributes and exemplars.
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding)},
		// The ID of the parent metric.
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint16},
		{Name: constants.StartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.AttributesID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding)},
		{Name: constants.MetricValue, Type: MetricValueDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Exemplars, Type: arrow.ListOf(ExemplarDT), Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Flags, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

type (
	// NumberDataPointBuilder is a builder for number data points.
	NumberDataPointBuilder struct {
		released bool

		builder *builder.RecordBuilderExt

		ib  *builder.Uint32DeltaBuilder // id builder
		pib *builder.Uint16Builder      // parent_id builder

		stunb *builder.TimestampBuilder // start_time_unix_nano builder
		tunb  *builder.TimestampBuilder // time_unix_nano builder
		mvb   *MetricValueBuilder       // metric_value builder
		elb   *builder.ListBuilder      // exemplars builder
		eb    *ExemplarBuilder          // exemplar builder
		fb    *builder.Uint32Builder    // flags builder

		accumulator *NDPAccumulator
		attrsAccu   *acommon.Attributes32Accumulator

		payloadType *acommon.PayloadType
	}

	// NDP is an internal representation of a number data point used by the
	// NDPAccumulator.
	NDP struct {
		ParentID          uint16
		Attributes        pcommon.Map
		SMData            *ScopeMetricsSharedData
		MData             *MetricSharedData
		StartTimeUnixNano pcommon.Timestamp
		TimeUnixNano      pcommon.Timestamp
		Orig              pmetric.NumberDataPoint
		Exemplars         pmetric.ExemplarSlice
		Flags             pmetric.DataPointFlags
	}

	// NDPAccumulator is an accumulator for number data points.
	NDPAccumulator struct {
		groupCount uint32
		ndps       []NDP
	}
)

// NewNumberDataPointBuilder creates a new NumberDataPointBuilder.
func NewNumberDataPointBuilder(rBuilder *builder.RecordBuilderExt, payloadType *acommon.PayloadType) *NumberDataPointBuilder {
	b := &NumberDataPointBuilder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewNDPAccumulator(),
		payloadType: payloadType,
	}

	b.init()
	return b
}

func (b *NumberDataPointBuilder) init() {
	b.ib = b.builder.Uint32DeltaBuilder(constants.ID)
	// As the attributes are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	b.ib.SetMaxDelta(1)
	b.pib = b.builder.Uint16Builder(constants.ParentID)

	b.stunb = b.builder.TimestampBuilder(constants.StartTimeUnixNano)
	b.tunb = b.builder.TimestampBuilder(constants.TimeUnixNano)
	b.mvb = MetricValueBuilderFrom(b.builder.SparseUnionBuilder(constants.MetricValue))
	b.elb = b.builder.ListBuilder(constants.Exemplars)
	b.eb = ExemplarBuilderFrom(b.elb.StructBuilder())
	b.fb = b.builder.Uint32Builder(constants.Flags)
}

func (b *NumberDataPointBuilder) SetAttributesAccumulator(accu *acommon.Attributes32Accumulator) {
	b.attrsAccu = accu
}

func (b *NumberDataPointBuilder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *NumberDataPointBuilder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *NumberDataPointBuilder) Accumulator() *NDPAccumulator {
	return b.accumulator
}

func (b *NumberDataPointBuilder) Build() (record arrow.Record, err error) {
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

func (b *NumberDataPointBuilder) TryBuild(attrsAccu *acommon.Attributes32Accumulator) (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	b.accumulator.Sort()

	for ID, ndp := range b.accumulator.ndps {
		b.ib.Append(uint32(ID))
		b.pib.Append(ndp.ParentID)

		// Attributes
		err = attrsAccu.AppendUniqueAttributesWithID(uint32(ID), ndp.Attributes, ndp.SMData.Attributes, ndp.MData.Attributes)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		if ndp.SMData.StartTime == nil && ndp.MData.StartTime == nil {
			b.stunb.Append(arrow.Timestamp(ndp.StartTimeUnixNano))
		} else {
			b.stunb.AppendNull()
		}
		if ndp.SMData.Time == nil && ndp.MData.Time == nil {
			b.tunb.Append(arrow.Timestamp(ndp.TimeUnixNano))
		} else {
			b.tunb.AppendNull()
		}
		if err = b.mvb.AppendNumberDataPointValue(ndp.Orig); err != nil {
			return nil, werror.Wrap(err)
		}

		b.fb.Append(uint32(ndp.Flags))

		ec := ndp.Exemplars.Len()
		err = b.elb.Append(ec, func() error {
			for i := 0; i < ec; i++ {
				if err = b.eb.Append(ndp.Exemplars.At(i)); err != nil {
					return werror.Wrap(err)
				}
			}
			return nil
		})
		if err != nil {
			return nil, werror.Wrap(err)
		}
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}
	return
}

func (b *NumberDataPointBuilder) Reset() {
	b.accumulator.Reset()
}

func (b *NumberDataPointBuilder) PayloadType() *acommon.PayloadType {
	return b.payloadType
}

// Release releases the underlying memory.
func (b *NumberDataPointBuilder) Release() {
	if b.released {
		return
	}
	b.builder.Release()
	b.released = true
}

// NewNDPAccumulator creates a new NDPAccumulator.
func NewNDPAccumulator() *NDPAccumulator {
	return &NDPAccumulator{
		groupCount: 0,
		ndps:       make([]NDP, 0),
	}
}

func (a *NDPAccumulator) IsEmpty() bool {
	return len(a.ndps) == 0
}

// Append appends a slice of number data points to the accumulator.
func (a *NDPAccumulator) Append(
	metricID uint16,
	ndps pmetric.NumberDataPointSlice,
	smdata *ScopeMetricsSharedData,
	mdata *MetricSharedData,
) error {
	if a.groupCount == math.MaxUint32 {
		panic("The maximum number of group of number of data points has been reached (max is uint32).")
	}

	if ndps.Len() == 0 {
		return nil
	}

	for i := 0; i < ndps.Len(); i++ {
		ndp := ndps.At(i)

		a.ndps = append(a.ndps, NDP{
			ParentID:          metricID,
			Attributes:        ndp.Attributes(),
			SMData:            smdata,
			MData:             mdata,
			TimeUnixNano:      ndp.Timestamp(),
			StartTimeUnixNano: ndp.StartTimestamp(),
			Orig:              ndp,
			Exemplars:         ndp.Exemplars(),
			Flags:             ndp.Flags(),
		})
	}

	a.groupCount++

	return nil
}

func (a *NDPAccumulator) Sort() {
	sort.Slice(a.ndps, func(i, j int) bool {
		if a.ndps[i].Orig.ValueType() == a.ndps[j].Orig.ValueType() {
			switch a.ndps[i].Orig.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				return a.ndps[i].Orig.IntValue() < a.ndps[j].Orig.IntValue()
			case pmetric.NumberDataPointValueTypeDouble:
				return a.ndps[i].Orig.DoubleValue() < a.ndps[j].Orig.DoubleValue()
			default:
				panic(fmt.Sprintf("unknown value type %d", a.ndps[i].Orig.ValueType()))
			}
		} else {
			return a.ndps[i].Orig.ValueType() < a.ndps[j].Orig.ValueType()
		}
	})
}

func (a *NDPAccumulator) Reset() {
	a.groupCount = 0
	a.ndps = a.ndps[:0]
}
