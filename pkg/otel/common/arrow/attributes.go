package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Arrow data types used to build the attribute map.
var (
	// KDT is the Arrow key data type.
	KDT = DictU16String

	// AttributesDT is the Arrow attribute data type.
	AttributesDT = arrow.MapOf(KDT, AnyValueDT)
)

// AttributesBuilder is a helper to build a map of attributes.
type AttributesBuilder struct {
	released bool

	builder *array.MapBuilder              // map builder
	kb      *array.BinaryDictionaryBuilder // key builder
	ib      *array.SparseUnionBuilder      // item builder

	strBuilder    *array.BinaryDictionaryBuilder
	i64Builder    *array.Int64Builder
	f64Builder    *array.Float64Builder
	boolBuilder   *array.BooleanBuilder
	binaryBuilder *array.BinaryDictionaryBuilder
}

// NewAttributesBuilder creates a new AttributesBuilder with a given allocator.
//
// Once the builder is no longer needed, Build() or Release() must be called to free the
// memory allocated by the builder.
func NewAttributesBuilder(pool *memory.GoAllocator) *AttributesBuilder {
	mb := array.NewMapBuilder(pool, KDT, AnyValueDT, false)
	return AttributesBuilderFrom(mb)
}

// AttributesBuilderFrom creates a new AttributesBuilder from an existing MapBuilder.
func AttributesBuilderFrom(mb *array.MapBuilder) *AttributesBuilder {
	ib := mb.ItemBuilder().(*array.SparseUnionBuilder)
	strBuilder := ib.Child(0).(*array.BinaryDictionaryBuilder)
	intBuilder := ib.Child(1).(*array.Int64Builder)
	doubleBuilder := ib.Child(2).(*array.Float64Builder)
	boolBuilder := ib.Child(3).(*array.BooleanBuilder)
	binaryBuilder := ib.Child(4).(*array.BinaryDictionaryBuilder)

	return &AttributesBuilder{
		released:      false,
		builder:       mb,
		kb:            mb.KeyBuilder().(*array.BinaryDictionaryBuilder),
		ib:            ib,
		strBuilder:    strBuilder,
		i64Builder:    intBuilder,
		f64Builder:    doubleBuilder,
		boolBuilder:   boolBuilder,
		binaryBuilder: binaryBuilder,
	}
}

// Build builds the attribute array map.
//
// Once the returned array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *AttributesBuilder) Build() (*array.Map, error) {
	if b.released {
		return nil, fmt.Errorf("attribute builder already released")
	}

	defer b.Release()
	return b.builder.NewMapArray(), nil
}

// Append appends a new set of attributes to the builder.
// Note: empty keys are skipped.
func (b *AttributesBuilder) Append(attrs pcommon.Map) error {
	if b.released {
		return fmt.Errorf("attribute builder already released")
	}

	// Count the number of non-empty key.
	nonEmptyKeyCount := 0
	attrs.Range(func(key string, v pcommon.Value) bool {
		if key != "" {
			nonEmptyKeyCount++
		}
		return true
	})

	if nonEmptyKeyCount == 0 {
		b.append0Attrs()
		return nil
	}
	b.appendNAttrs(nonEmptyKeyCount)

	var err error
	attrs.Range(func(key string, v pcommon.Value) bool {
		// Skip empty key.
		if key == "" {
			return true
		}

		switch v.Type() {
		case pcommon.ValueTypeEmpty:
			b.append0Attrs()
		case pcommon.ValueTypeStr:
			err = b.appendStr(key, v.Str())
		case pcommon.ValueTypeInt:
			err = b.appendI64(key, v.Int())
		case pcommon.ValueTypeDouble:
			err = b.appendF64(key, v.Double())
		case pcommon.ValueTypeBool:
			err = b.appendBool(key, v.Bool())
		case pcommon.ValueTypeBytes:
			err = b.appendBinary(key, v.Bytes().AsRaw())
		case pcommon.ValueTypeSlice:
			err = fmt.Errorf("slice value type not supported")
		case pcommon.ValueTypeMap:
			err = fmt.Errorf("map value type not supported")
		}
		if err != nil {
			return false
		}
		return true
	})
	return err
}

// Release releases the memory allocated by the builder.
func (b *AttributesBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.kb.Release()
		b.ib.Release()

		b.strBuilder.Release()
		b.i64Builder.Release()
		b.f64Builder.Release()
		b.boolBuilder.Release()
		b.binaryBuilder.Release()

		b.released = true
	}
}

// appendNAttrs appends a new set of key-value pairs to the builder.
func (b *AttributesBuilder) appendNAttrs(count int) {
	b.builder.Append(true)
	b.builder.Reserve(count)
}

// append0Attrs appends an empty set of key-value pairs to the builder.
func (b *AttributesBuilder) append0Attrs() {
	b.builder.AppendNull()
}

// appendStr appends a new string attribute to the builder.
func (b *AttributesBuilder) appendStr(k string, v string) error {
	err := b.kb.AppendString(k)
	if err != nil {
		return err
	}
	b.ib.Append(StrCode)
	if v == "" {
		b.strBuilder.AppendNull()
	} else {
		if err := b.strBuilder.AppendString(v); err != nil {
			return err
		}
	}
	b.i64Builder.AppendNull()
	b.f64Builder.AppendNull()
	b.boolBuilder.AppendNull()
	b.binaryBuilder.AppendNull()

	return nil
}

// appendI64 appends a new int64 attribute to the builder.
func (b *AttributesBuilder) appendI64(k string, v int64) error {
	err := b.kb.AppendString(k)
	if err != nil {
		return err
	}
	b.ib.Append(I64Code)
	b.i64Builder.Append(v)

	b.strBuilder.AppendNull()
	b.f64Builder.AppendNull()
	b.boolBuilder.AppendNull()
	b.binaryBuilder.AppendNull()

	return nil
}

// appendF64 appends a new double attribute to the builder.
func (b *AttributesBuilder) appendF64(k string, v float64) error {
	err := b.kb.AppendString(k)
	if err != nil {
		return err
	}
	b.ib.Append(F64Code)
	b.f64Builder.Append(v)

	b.strBuilder.AppendNull()
	b.i64Builder.AppendNull()
	b.boolBuilder.AppendNull()
	b.binaryBuilder.AppendNull()

	return nil
}

// appendBool appends a new bool attribute to the builder.
func (b *AttributesBuilder) appendBool(k string, v bool) error {
	err := b.kb.AppendString(k)
	if err != nil {
		return err
	}
	b.ib.Append(BoolCode)
	b.boolBuilder.Append(v)

	b.strBuilder.AppendNull()
	b.i64Builder.AppendNull()
	b.f64Builder.AppendNull()
	b.binaryBuilder.AppendNull()

	return nil
}

// appendBinary appends a new binary attribute to the builder.
func (b *AttributesBuilder) appendBinary(k string, v []byte) error {
	err := b.kb.AppendString(k)
	if err != nil {
		return err
	}
	b.ib.Append(BinaryCode)
	if v == nil {
		b.binaryBuilder.AppendNull()
	} else {
		if err := b.binaryBuilder.Append(v); err != nil {
			return err
		}
	}

	b.strBuilder.AppendNull()
	b.i64Builder.AppendNull()
	b.f64Builder.AppendNull()
	b.boolBuilder.AppendNull()

	return nil
}
