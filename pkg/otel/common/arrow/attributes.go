package arrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Constants used to identify the type of value in the union.
const (
	StrCode    int8 = 0
	IntCode    int8 = 1
	DoubleCode int8 = 2
	BoolCode   int8 = 3
	BinaryCode int8 = 4
)

// Array data types used to build the attribute map.
var (
	KDT = arrow.BinaryTypes.String
	IDT = arrow.DenseUnionOf([]arrow.Field{
		{Name: "string", Type: arrow.BinaryTypes.String},
		{Name: "int", Type: arrow.PrimitiveTypes.Int64},
		{Name: "double", Type: arrow.PrimitiveTypes.Float64},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "binary", Type: arrow.BinaryTypes.Binary},
	}, []int8{
		StrCode,
		IntCode,
		DoubleCode,
		BoolCode,
		BinaryCode,
	})
	AttributesDT = arrow.MapOf(KDT, IDT)
)

// AttributesBuilder is a helper to build a map of attributes.
type AttributesBuilder struct {
	released bool

	builder *array.MapBuilder
	kb      *array.StringBuilder
	ib      *array.DenseUnionBuilder

	strBuilder    *array.StringBuilder
	intBuilder    *array.Int64Builder
	doubleBuilder *array.Float64Builder
	boolBuilder   *array.BooleanBuilder
	binaryBuilder *array.BinaryBuilder
}

// NewAttributesBuilder creates a new AttributesBuilder with a given allocator.
//
// Once the builder is no longer needed, Release() must be called to free the
// memory allocated by the builder.
func NewAttributesBuilder(pool *memory.GoAllocator) *AttributesBuilder {
	// TODO: Is it better to sort the keys?
	mb := array.NewMapBuilder(pool, KDT, IDT, false)
	return AttributesBuilderFrom(mb)
}

func AttributesBuilderFrom(mb *array.MapBuilder) *AttributesBuilder {
	ib := mb.ItemBuilder().(*array.DenseUnionBuilder)
	strBuilder := ib.Child(0).(*array.StringBuilder)
	intBuilder := ib.Child(1).(*array.Int64Builder)
	doubleBuilder := ib.Child(2).(*array.Float64Builder)
	boolBuilder := ib.Child(3).(*array.BooleanBuilder)
	binaryBuilder := ib.Child(4).(*array.BinaryBuilder)

	return &AttributesBuilder{
		released:      false,
		builder:       mb,
		kb:            mb.KeyBuilder().(*array.StringBuilder),
		ib:            ib,
		strBuilder:    strBuilder,
		intBuilder:    intBuilder,
		doubleBuilder: doubleBuilder,
		boolBuilder:   boolBuilder,
		binaryBuilder: binaryBuilder,
	}
}

// Build builds the attribute array map.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *AttributesBuilder) Build() *array.Map {
	if b.released {
		panic("attribute builder already released")
	}

	defer b.Release()
	return b.builder.NewMapArray()
}

// Append appends a new set of attributes to the builder.
//
// This method panics if the builder has already been released.
func (b *AttributesBuilder) Append(attrs pcommon.Map) {
	if b.released {
		panic("attribute builder already released")
	}

	if attrs.Len() == 0 {
		b.append0Attrs()
		return
	}
	b.appendNAttrs()

	attrs.Range(func(key string, v pcommon.Value) bool {
		switch v.Type() {
		case pcommon.ValueTypeEmpty:
			b.append0Attrs()
		case pcommon.ValueTypeStr:
			b.appendStr(key, v.Str())
		case pcommon.ValueTypeInt:
			b.appendInt(key, v.Int())
		case pcommon.ValueTypeDouble:
			b.appendDouble(key, v.Double())
		case pcommon.ValueTypeBool:
			b.appendBool(key, v.Bool())
		case pcommon.ValueTypeBytes:
			b.appendBinary(key, v.Bytes().AsRaw())
		case pcommon.ValueTypeSlice:
			// Not yet supported
		case pcommon.ValueTypeMap:
			// Not yet supported
		}
		return true
	})
}

// Release releases the memory allocated by the builder.
func (b *AttributesBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.kb.Release()
		b.ib.Release()

		b.strBuilder.Release()
		b.intBuilder.Release()
		b.doubleBuilder.Release()
		b.boolBuilder.Release()
		b.binaryBuilder.Release()

		b.released = true
	}
}

// appendNAttrs appends a new set of key-value pairs to the builder.
func (b *AttributesBuilder) appendNAttrs() {
	b.builder.Append(true)
}

// append0Attrs appends an empty set of key-value pairs to the builder.
func (b *AttributesBuilder) append0Attrs() {
	b.builder.AppendNull()
}

// appendStr appends a new string attribute to the builder.
func (b *AttributesBuilder) appendStr(k string, v string) {
	b.kb.Append(k)
	b.ib.Append(StrCode)
	b.strBuilder.Append(v)
}

// appendInt appends a new int attribute to the builder.
func (b *AttributesBuilder) appendInt(k string, v int64) {
	b.kb.Append(k)
	b.ib.Append(IntCode)
	b.intBuilder.Append(v)
}

// appendDouble appends a new double attribute to the builder.
func (b *AttributesBuilder) appendDouble(k string, v float64) {
	b.kb.Append(k)
	b.ib.Append(DoubleCode)
	b.doubleBuilder.Append(v)
}

// appendBool appends a new bool attribute to the builder.
func (b *AttributesBuilder) appendBool(k string, v bool) {
	b.kb.Append(k)
	b.ib.Append(BoolCode)
	b.boolBuilder.Append(v)
}

// appendBinary appends a new binary attribute to the builder.
func (b *AttributesBuilder) appendBinary(k string, v []byte) {
	b.kb.Append(k)
	b.ib.Append(BinaryCode)
	b.binaryBuilder.Append(v)
}
