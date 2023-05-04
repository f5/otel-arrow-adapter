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
	"sort"

	"github.com/apache/arrow/go/v12/arrow"
	"go.opentelemetry.io/collector/pdata/pmetric"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

var (
	// DoubleDataPointSchema is the Arrow schema representing double data
	// points.
	// Related record.
	DoubleDataPointSchema = arrow.NewSchema([]arrow.Field{
		// This unique identifier is used to identify the relationship between
		// the double data point, its attributes and exemplars.
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.DeltaEncoding)},
		// The ID of the parent metric.
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint16},
		{Name: constants.StartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: constants.MetricValue, Type: arrow.PrimitiveTypes.Float64},
		{Name: constants.Exemplars, Type: arrow.ListOf(ExemplarDT), Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Flags, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

type (
	// DoubleDataPointBuilder is a builder for number data points.
	DoubleDataPointBuilder struct {
		released bool

		builder *builder.RecordBuilderExt

		ib  *builder.Uint32DeltaBuilder // id builder
		pib *builder.Uint16Builder      // parent_id builder

		stunb *builder.TimestampBuilder // start_time_unix_nano builder
		tunb  *builder.TimestampBuilder // time_unix_nano builder
		mvb   *builder.Float64Builder   // metric_value builder
		elb   *builder.ListBuilder      // exemplars builder
		eb    *ExemplarBuilder          // exemplar builder
		fb    *builder.Uint32Builder    // flags builder

		accumulator *DDPAccumulator
		attrsAccu   *acommon.Attributes32Accumulator

		payloadType *acommon.PayloadType
	}

	// DDP is an internal representation of a double data point used by the
	// DDPAccumulator.
	DDP struct {
		ParentID uint16
		Orig     *pmetric.NumberDataPoint
	}

	// DDPAccumulator is an accumulator for double data points.
	DDPAccumulator struct {
		dps []IDP
	}
)

// NewDoubleDataPointBuilder creates a new DoubleDataPointBuilder.
func NewDoubleDataPointBuilder(rBuilder *builder.RecordBuilderExt, payloadType *acommon.PayloadType) *DoubleDataPointBuilder {
	b := &DoubleDataPointBuilder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewDDPAccumulator(),
		payloadType: payloadType,
	}

	b.init()
	return b
}

func (b *DoubleDataPointBuilder) init() {
	b.ib = b.builder.Uint32DeltaBuilder(constants.ID)
	// As the attributes are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	b.ib.SetMaxDelta(1)
	b.pib = b.builder.Uint16Builder(constants.ParentID)

	b.stunb = b.builder.TimestampBuilder(constants.StartTimeUnixNano)
	b.tunb = b.builder.TimestampBuilder(constants.TimeUnixNano)
	b.mvb = b.builder.Float64Builder(constants.MetricValue)
	b.elb = b.builder.ListBuilder(constants.Exemplars)
	b.eb = ExemplarBuilderFrom(b.elb.StructBuilder())
	b.fb = b.builder.Uint32Builder(constants.Flags)
}

func (b *DoubleDataPointBuilder) SetAttributesAccumulator(accu *acommon.Attributes32Accumulator) {
	b.attrsAccu = accu
}

func (b *DoubleDataPointBuilder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *DoubleDataPointBuilder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *DoubleDataPointBuilder) Accumulator() *DDPAccumulator {
	return b.accumulator
}

func (b *DoubleDataPointBuilder) Build() (record arrow.Record, err error) {
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

func (b *DoubleDataPointBuilder) TryBuild(attrsAccu *acommon.Attributes32Accumulator) (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	b.accumulator.Sort()

	for ID, ndp := range b.accumulator.dps {
		b.ib.Append(uint32(ID))
		b.pib.Append(ndp.ParentID)

		// Attributes
		err = attrsAccu.AppendUniqueAttributesWithID(uint32(ID), ndp.Orig.Attributes(), nil, nil)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		startTime := ndp.Orig.StartTimestamp()
		if startTime == 0 {
			b.stunb.AppendNull()
		} else {
			b.stunb.Append(arrow.Timestamp(startTime))
		}
		b.tunb.Append(arrow.Timestamp(ndp.Orig.Timestamp()))
		b.mvb.Append(ndp.Orig.DoubleValue())
		b.fb.Append(uint32(ndp.Orig.Flags()))

		exemplars := ndp.Orig.Exemplars()
		ec := exemplars.Len()
		err = b.elb.Append(ec, func() error {
			for i := 0; i < ec; i++ {
				if err = b.eb.Append(exemplars.At(i)); err != nil {
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

func (b *DoubleDataPointBuilder) Reset() {
	b.accumulator.Reset()
}

func (b *DoubleDataPointBuilder) PayloadType() *acommon.PayloadType {
	return b.payloadType
}

// Release releases the underlying memory.
func (b *DoubleDataPointBuilder) Release() {
	if b.released {
		return
	}
	b.builder.Release()
	b.released = true
}

// NewDDPAccumulator creates a new DDPAccumulator.
func NewDDPAccumulator() *DDPAccumulator {
	return &DDPAccumulator{
		dps: make([]IDP, 0),
	}
}

func (a *DDPAccumulator) IsEmpty() bool {
	return len(a.dps) == 0
}

// Append appends a slice of number data points to the accumulator.
func (a *DDPAccumulator) Append(
	metricID uint16,
	dp pmetric.NumberDataPoint,
) {
	a.dps = append(a.dps, IDP{
		ParentID: metricID,
		Orig:     &dp,
	})
}

func (a *DDPAccumulator) Sort() {
	sort.Slice(a.dps, func(i, j int) bool {
		if a.dps[i].Orig.Timestamp() == a.dps[j].Orig.Timestamp() {
			return a.dps[i].Orig.DoubleValue() < a.dps[j].Orig.DoubleValue()
		} else {
			return a.dps[i].Orig.Timestamp() < a.dps[j].Orig.Timestamp()
		}
	})
}

func (a *DDPAccumulator) Reset() {
	a.dps = a.dps[:0]
}
