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

package arrow_old

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"

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

	Attr16 struct {
		ID    uint16
		Key   string
		Value pcommon.Value
	}

	Attr32 struct {
		ID    uint32
		Key   string
		Value pcommon.Value
	}

	// Attributes16Accumulator accumulates attributes for the scope of an entire
	// batch. It is used to sort globally all attributes and optimize the
	// compression ratio. Attribute IDs are 16-bit.
	Attributes16Accumulator struct {
		attrsMapCount uint16
		attrs         []Attr16
	}

	// Attributes32Accumulator accumulates attributes for the scope of an entire
	// batch. It is used to sort globally all attributes and optimize the
	// compression ratio. Attribute IDs are 32-bit.
	Attributes32Accumulator struct {
		attrsMapCount uint32
		attrs         []Attr32
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

func NewAttributes16Accumulator() *Attributes16Accumulator {
	return &Attributes16Accumulator{
		attrs: make([]Attr16, 0),
	}
}

func (c *Attributes16Accumulator) IsEmpty() bool {
	return len(c.attrs) == 0
}

func (c *Attributes16Accumulator) Append(attrs pcommon.Map) (int64, error) {
	ID := c.attrsMapCount

	if attrs.Len() == 0 {
		return -1, nil
	}

	if c.attrsMapCount == math.MaxUint16 {
		panic("The maximum number of group of attributes has been reached (max is uint16).")
	}

	attrs.Range(func(k string, v pcommon.Value) bool {
		c.attrs = append(c.attrs, Attr16{
			ID:    ID,
			Key:   k,
			Value: v,
		})
		return true
	})

	c.attrsMapCount++

	return int64(ID), nil
}

func (c *Attributes16Accumulator) AppendUniqueAttributesWithID(mainID uint16, attrs pcommon.Map, smattrs *common.SharedAttributes, mattrs *common.SharedAttributes) error {
	uniqueAttrsCount := attrs.Len()
	if smattrs != nil {
		uniqueAttrsCount -= smattrs.Len()
	}
	if mattrs != nil {
		uniqueAttrsCount -= mattrs.Len()
	}

	if uniqueAttrsCount == 0 {
		return nil
	}

	if c.attrsMapCount == math.MaxUint16 {
		panic("The maximum number of group of attributes has been reached (max is uint16).")
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

		c.attrs = append(c.attrs, Attr16{
			ID:    mainID,
			Key:   key,
			Value: v,
		})

		uniqueAttrsCount--
		return uniqueAttrsCount > 0
	})

	c.attrsMapCount++

	return nil
}

func (c *Attributes16Accumulator) AppendUniqueAttributes(attrs pcommon.Map, smattrs *common.SharedAttributes, mattrs *common.SharedAttributes) (int64, error) {
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

	if c.attrsMapCount == math.MaxUint16 {
		panic("The maximum number of group of attributes has been reached (max is uint16).")
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

		c.attrs = append(c.attrs, Attr16{
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

func (c *Attributes16Accumulator) SortedAttrs() []Attr16 {
	sort.Slice(c.attrs, func(i, j int) bool {
		attrsI := c.attrs[i]
		attrsJ := c.attrs[j]
		if attrsI.Key == attrsJ.Key {
			cmp := Compare(attrsI.Value, attrsJ.Value)
			if cmp == 0 {
				return attrsI.ID < attrsJ.ID
			} else {
				return cmp == -1
			}
		} else {
			return attrsI.Key < attrsJ.Key
		}
	})

	return c.attrs
}

func (c *Attributes16Accumulator) Reset() {
	c.attrsMapCount = 0
	c.attrs = c.attrs[:0]
}

func NewAttributes32Accumulator() *Attributes32Accumulator {
	return &Attributes32Accumulator{
		attrs: make([]Attr32, 0),
	}
}

func (c *Attributes32Accumulator) IsEmpty() bool {
	return len(c.attrs) == 0
}

func (c *Attributes32Accumulator) Append(attrs pcommon.Map) (int64, error) {
	ID := c.attrsMapCount

	if attrs.Len() == 0 {
		return -1, nil
	}

	if c.attrsMapCount == math.MaxUint32 {
		panic("The maximum number of group of attributes has been reached (max is uint32).")
	}

	attrs.Range(func(k string, v pcommon.Value) bool {
		c.attrs = append(c.attrs, Attr32{
			ID:    ID,
			Key:   k,
			Value: v,
		})
		return true
	})

	c.attrsMapCount++

	return int64(ID), nil
}

func (c *Attributes32Accumulator) AppendUniqueAttributes(attrs pcommon.Map, smattrs *common.SharedAttributes, mattrs *common.SharedAttributes) (int64, error) {
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

	if c.attrsMapCount == math.MaxUint32 {
		panic("The maximum number of group of attributes has been reached (max is uint32).")
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

		c.attrs = append(c.attrs, Attr32{
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

func (c *Attributes32Accumulator) SortedAttrs() []Attr32 {
	sort.Slice(c.attrs, func(i, j int) bool {
		attrsI := c.attrs[i]
		attrsJ := c.attrs[j]
		if attrsI.ID == attrsJ.ID {
			if attrsI.Key == attrsJ.Key {
				return IsLess(attrsI.Value, attrsJ.Value)
			} else {
				return attrsI.Key < attrsJ.Key
			}
		} else {
			return attrsI.ID < attrsJ.ID
		}
	})

	return c.attrs
}

func (c *Attributes32Accumulator) Reset() {
	c.attrsMapCount = 0
	c.attrs = c.attrs[:0]
}

func IsLess(a, b pcommon.Value) bool {
	switch a.Type() {
	case pcommon.ValueTypeInt:
		if b.Type() == pcommon.ValueTypeInt {
			return a.Int() < b.Int()
		} else {
			return false
		}
	case pcommon.ValueTypeDouble:
		if b.Type() == pcommon.ValueTypeDouble {
			return a.Double() < b.Double()
		} else {
			return false
		}
	case pcommon.ValueTypeBool:
		return a.Bool() == true && b.Bool() == false
	case pcommon.ValueTypeStr:
		if b.Type() == pcommon.ValueTypeStr {
			return a.Str() < b.Str()
		} else {
			return false
		}
	case pcommon.ValueTypeBytes:
		if a.Type() == pcommon.ValueTypeBytes && b.Type() == pcommon.ValueTypeBytes {
			return bytes.Compare(a.Bytes().AsRaw(), b.Bytes().AsRaw()) < 0
		} else {
			return false
		}
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

func Compare(a, b pcommon.Value) int {
	switch a.Type() {
	case pcommon.ValueTypeInt:
		aI := a.Int()
		if b.Type() == pcommon.ValueTypeInt {
			bI := b.Int()
			if aI == bI {
				return 0
			} else if aI < bI {
				return -1
			} else {
				return 1
			}
		} else {
			return 1
		}
	case pcommon.ValueTypeDouble:
		aD := a.Double()
		if b.Type() == pcommon.ValueTypeDouble {
			bD := b.Double()
			if aD == bD {
				return 0
			} else if aD < bD {
				return -1
			} else {
				return 1
			}
		} else {
			return 1
		}
	case pcommon.ValueTypeBool:
		aB := a.Bool()
		bB := b.Bool()
		if aB == bB {
			return 0
		} else if aB == true && bB == false {
			return 1
		} else {
			return -1
		}
	case pcommon.ValueTypeStr:
		if b.Type() == pcommon.ValueTypeStr {
			return strings.Compare(a.Str(), b.Str())
		} else {
			return 1
		}
	case pcommon.ValueTypeBytes:
		if a.Type() == pcommon.ValueTypeBytes && b.Type() == pcommon.ValueTypeBytes {
			return bytes.Compare(a.Bytes().AsRaw(), b.Bytes().AsRaw())
		} else {
			return 1
		}
	case pcommon.ValueTypeMap:
		return 1
	case pcommon.ValueTypeSlice:
		return 1
	case pcommon.ValueTypeEmpty:
		return 1
	default:
		return 1
	}
}
