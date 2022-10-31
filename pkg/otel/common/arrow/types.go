package arrow

import "github.com/apache/arrow/go/v10/arrow"

var (
	Dict16String = &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: arrow.BinaryTypes.String,
		Ordered:   false,
	}

	Dict16Binary = &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: arrow.BinaryTypes.Binary,
		Ordered:   false,
	}

	Dict16Fixed16Binary = &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: &arrow.FixedSizeBinaryType{ByteWidth: 16},
		Ordered:   false,
	}

	Dict16Fixed8Binary = &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: &arrow.FixedSizeBinaryType{ByteWidth: 8},
		Ordered:   false,
	}
)
