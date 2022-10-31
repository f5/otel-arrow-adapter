package arrow

import "github.com/apache/arrow/go/v10/arrow"

var (
	Dict16String = &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: arrow.BinaryTypes.String,
		Ordered:   false,
	}
)
