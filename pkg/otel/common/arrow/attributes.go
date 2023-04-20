/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package arrow

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/exp/rand"

	arrowutils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

// Arrow data types used to build the attribute map.
var (
	// KDT is the Arrow key data type.
	KDT = arrow.BinaryTypes.String

	// AttributesDT is the Arrow attribute data type.
	AttributesDT = arrow.MapOfWithMetadata(
		KDT, schema.Metadata(schema.Dictionary8),
		AnyValueDT, schema.Metadata(),
	)
)

type (
	AttributesStats struct {
		AttrsHistogram *hdrhistogram.Histogram
		AnyValueStats  *AnyValueStats
	}

	// AttributesBuilder is a helper to build a map of attributes.
	AttributesBuilder struct {
		released bool

		builder *builder.MapBuilder    // map builder
		kb      *builder.StringBuilder // key builder
		ib      *AnyValueBuilder       // item any value builder
	}

	Attr struct {
		ID    uint32
		Key   string
		Value pcommon.Value
	}

	// AttributesAccumulator accumulates attributes for the scope of an entire
	// batch. It is used to sort globally all attributes and optimize the
	// the compression ratio.
	AttributesAccumulator struct {
		attrsMapCount uint32
		attrs         []Attr
	}
)

// NewAttributesBuilder creates a new AttributesBuilder with a given allocator.
//
// Once the builder is no longer needed, Build() or Release() must be called to free the
// memory allocated by the builder.
func NewAttributesBuilder(builder *builder.MapBuilder) *AttributesBuilder {
	return AttributesBuilderFrom(builder)
}

// AttributesBuilderFrom creates a new AttributesBuilder from an existing MapBuilder.
func AttributesBuilderFrom(mb *builder.MapBuilder) *AttributesBuilder {
	ib := AnyValueBuilderFrom(mb.ItemSparseUnionBuilder())

	return &AttributesBuilder{
		released: false,
		builder:  mb,
		kb:       mb.KeyStringBuilder(),
		ib:       ib,
	}
}

// Build builds the attribute array map.
//
// Once the returned array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *AttributesBuilder) Build() (*array.Map, error) {
	if b.released {
		return nil, werror.Wrap(ErrBuilderAlreadyReleased)
	}

	defer b.Release()
	return b.builder.NewMapArray(), nil
}

func (b *AttributesBuilder) AppendNull() error {
	if b.released {
		return werror.Wrap(ErrBuilderAlreadyReleased)
	}

	b.builder.AppendNull()
	return nil
}

// Append appends a new set of attributes to the builder.
// Note: empty keys are skipped.
func (b *AttributesBuilder) Append(attrs pcommon.Map) error {
	if b.released {
		return werror.Wrap(ErrBuilderAlreadyReleased)
	}

	return b.builder.Append(attrs.Len(), func() error {
		var err error
		attrs.Range(func(key string, v pcommon.Value) bool {
			if key == "" {
				// Skip entries with empty keys
				return true
			}
			b.kb.AppendNonEmpty(key)
			return b.ib.Append(v) == nil
		})
		return werror.Wrap(err)
	})
}

func (b *AttributesBuilder) AppendUniqueAttributes(attrs pcommon.Map, smattrs *common.SharedAttributes, mattrs *common.SharedAttributes) error {
	if b.released {
		return werror.Wrap(ErrBuilderAlreadyReleased)
	}

	uniqueAttrsCount := attrs.Len()
	if smattrs != nil {
		uniqueAttrsCount -= smattrs.Len()
	}
	if mattrs != nil {
		uniqueAttrsCount -= mattrs.Len()
	}

	return b.builder.Append(uniqueAttrsCount, func() error {
		var err error

		attrs.Range(func(key string, v pcommon.Value) bool {
			if key == "" {
				// Skip entries with empty keys
				return true
			}

			// Skip the current attribute if it is a scope metric shared attribute
			// or a metric shared attribute
			smattrsFound := false
			mattrsFound := false
			if smattrs != nil {
				_, smattrsFound = smattrs.Attributes[key]
			}
			if mattrs != nil {
				_, mattrsFound = mattrs.Attributes[key]
			}
			if smattrsFound || mattrsFound {
				return true
			}

			b.kb.AppendNonEmpty(key)
			err = werror.WrapWithContext(b.ib.Append(v), map[string]interface{}{"key": key, "value": v})

			uniqueAttrsCount--
			return err == nil && uniqueAttrsCount > 0
		})

		return err
	})
}

// Release releases the memory allocated by the builder.
func (b *AttributesBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}

func NewAttributesStats() *AttributesStats {
	return &AttributesStats{
		AttrsHistogram: hdrhistogram.New(0, 1000000, 1),
		AnyValueStats:  NewAnyValueStats(),
	}
}

func (a *AttributesStats) UpdateStats(attrs pcommon.Map) {
	counters := ValueTypeCounters{}

	if err := a.AttrsHistogram.RecordValue(int64(attrs.Len())); err != nil {
		panic(fmt.Sprintf("failed to record attrs count: %v", err))
	}

	attrs.Range(func(key string, v pcommon.Value) bool {
		a.AnyValueStats.UpdateStats(v, &counters)
		return true
	})

	if err := a.AnyValueStats.StrHistogram.RecordValue(counters.strCount); err != nil {
		panic(fmt.Sprintf("failed to record str count: %v", err))
	}
	if err := a.AnyValueStats.I64Histogram.RecordValue(counters.i64Count); err != nil {
		panic(fmt.Sprintf("failed to record i64 count: %v", err))
	}
	if err := a.AnyValueStats.F64Histogram.RecordValue(counters.f64Count); err != nil {
		panic(fmt.Sprintf("failed to record f64 count: %v", err))
	}
	if err := a.AnyValueStats.BoolHistogram.RecordValue(counters.boolCount); err != nil {
		panic(fmt.Sprintf("failed to record bool count: %v", err))
	}
	if err := a.AnyValueStats.BinaryHistogram.RecordValue(counters.binaryCount); err != nil {
		panic(fmt.Sprintf("failed to record binary count: %v", err))
	}
	if err := a.AnyValueStats.ListHistogram.RecordValue(counters.listCount); err != nil {
		panic(fmt.Sprintf("failed to record list count: %v", err))
	}
	if err := a.AnyValueStats.MapHistogram.RecordValue(counters.mapCount); err != nil {
		panic(fmt.Sprintf("failed to record map count: %v", err))
	}
}

func (a *AttributesStats) Show(prefix string) {
	if a.AttrsHistogram.Mean() == 0 {
		return
	}
	fmt.Printf("%sAttributes -> mean: %8.2f, min: %8d, max: %8d, std-dev: %8.2f, p50: %8d, p99: %8d\n",
		prefix,
		a.AttrsHistogram.Mean(),
		a.AttrsHistogram.Min(),
		a.AttrsHistogram.Max(),
		a.AttrsHistogram.StdDev(),
		a.AttrsHistogram.ValueAtQuantile(50),
		a.AttrsHistogram.ValueAtQuantile(99),
	)
	a.AnyValueStats.Show(prefix + "  ")
}

func NewAttributesAccumulator() *AttributesAccumulator {
	return &AttributesAccumulator{
		attrs: make([]Attr, 0),
	}
}

func (c *AttributesAccumulator) IsEmpty() bool {
	return len(c.attrs) == 0
}

func (c *AttributesAccumulator) Append(attrs pcommon.Map) (int64, error) {
	ID := c.attrsMapCount

	if attrs.Len() == 0 {
		return -1, nil
	}

	attrs.Range(func(k string, v pcommon.Value) bool {
		c.attrs = append(c.attrs, Attr{
			ID:    ID,
			Key:   k,
			Value: v,
		})
		return true
	})

	c.attrsMapCount++

	return int64(ID), nil
}

func (c *AttributesAccumulator) AppendUniqueAttributes(attrs pcommon.Map, smattrs *common.SharedAttributes, mattrs *common.SharedAttributes) (int64, error) {
	uniqueAttrsCount := attrs.Len()
	if smattrs != nil {
		uniqueAttrsCount -= smattrs.Len()
	}
	if mattrs != nil {
		uniqueAttrsCount -= mattrs.Len()
	}

	ID := c.attrsMapCount
	if uniqueAttrsCount == 0 {
		return -1, nil
	}

	attrs.Range(func(key string, v pcommon.Value) bool {
		if key == "" {
			// Skip entries with empty keys
			return true
		}

		// Skip the current attribute if it is a scope metric shared attribute
		// or a metric shared attribute
		smattrsFound := false
		mattrsFound := false
		if smattrs != nil {
			_, smattrsFound = smattrs.Attributes[key]
		}
		if mattrs != nil {
			_, mattrsFound = mattrs.Attributes[key]
		}
		if smattrsFound || mattrsFound {
			return true
		}

		c.attrs = append(c.attrs, Attr{
			ID:    ID,
			Key:   key,
			Value: v,
		})

		uniqueAttrsCount--
		return uniqueAttrsCount > 0
	})

	c.attrsMapCount++

	return int64(ID), nil
}

func (c *AttributesAccumulator) SortedAttrs() []Attr {
	sort.Slice(c.attrs, func(i, j int) bool {
		if c.attrs[i].Key == c.attrs[j].Key {
			return IsLess(c.attrs[i].Value, c.attrs[j].Value)
		} else {
			return c.attrs[i].Key < c.attrs[j].Key
		}
	})

	return c.attrs
}

func (c *AttributesAccumulator) Reset() {
	c.attrsMapCount = 0
	c.attrs = c.attrs[:0]
}

func IsLess(a, b pcommon.Value) bool {
	switch a.Type() {
	case pcommon.ValueTypeInt:
		return a.Int() < b.Int()
	case pcommon.ValueTypeDouble:
		return a.Double() < b.Double()
	case pcommon.ValueTypeBool:
		return a.Bool() == true && b.Bool() == false
	case pcommon.ValueTypeStr:
		return a.Str() < b.Str()
	case pcommon.ValueTypeBytes:
		return bytes.Compare(a.Bytes().AsRaw(), b.Bytes().AsRaw()) < 0
	case pcommon.ValueTypeMap:
		return false
	case pcommon.ValueTypeSlice:
		return false
	case pcommon.ValueTypeEmpty:
		return false
	default:
		return false
	}
}

func PrintRecord(record arrow.Record) {
	print("\n")
	for _, field := range record.Schema().Fields() {
		print(field.Name + "\t\t")
	}
	print("\n")

	// Select a window of 1000 consecutive rows randomly from the record
	row := rand.Intn(int(record.NumRows()) - 1000)

	record = record.NewSlice(int64(row), int64(row+1000))
	numRows := int(record.NumRows())
	numCols := int(record.NumCols())

	for row := 0; row < numRows; row++ {
		for col := 0; col < numCols; col++ {
			col := record.Column(col)
			if col.IsNull(row) {
				print("null")
			} else {
				switch c := col.(type) {
				case *array.Uint32:
					print(c.Value(row))
				case *array.Uint16:
					print(c.Value(row))
				case *array.Int64:
					print(c.Value(row))
				case *array.String:
					print(c.Value(row))
				case *array.Float64:
					print(c.Value(row))
				case *array.Boolean:
					print(c.Value(row))
				case *array.Binary:
					print(fmt.Sprintf("%x", c.Value(row)))
				case *array.Dictionary:
					switch d := c.Dictionary().(type) {
					case *array.String:
						print(d.Value(c.GetValueIndex(row)))
					case *array.Binary:
						print(fmt.Sprintf("%x", d.Value(c.GetValueIndex(row))))
					default:
						print("unknown dict type")
					}
				case *array.SparseUnion:
					tcode := c.TypeCode(row)
					fieldID := c.ChildID(row)
					switch tcode {
					case StrCode:
						strArr := c.Field(fieldID)
						val, err := arrowutils.StringFromArray(strArr, row)
						if err != nil {
							panic(err)
						}
						print(val)
					case I64Code:
						i64Arr := c.Field(fieldID)
						val := i64Arr.(*array.Int64).Value(row)
						print(val)
					case F64Code:
						f64Arr := c.Field(fieldID)
						val := f64Arr.(*array.Float64).Value(row)
						print(val)
					case BoolCode:
						boolArr := c.Field(fieldID)
						val := boolArr.(*array.Boolean).Value(row)
						print(val)
					case BinaryCode:
						binArr := c.Field(fieldID)
						val, err := arrowutils.BinaryFromArray(binArr, row)
						if err != nil {
							panic(err)
						}
						print(fmt.Sprintf("%x", val))
					default:
						fmt.Print("unknown type")
					}
				default:
					fmt.Print("unknown type")
				}
			}
			print("\t\t")
		}
		print("\n")
	}
}
