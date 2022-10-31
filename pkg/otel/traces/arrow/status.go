package arrow

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// StatusDT is the Arrow Data Type describing a span status.
var (
	StatusDT = arrow.StructOf([]arrow.Field{
		{Name: constants.STATUS_CODE, Type: arrow.PrimitiveTypes.Int32},
		{Name: constants.STATUS_MESSAGE, Type: acommon.Dict16String},
	}...)
)

type StatusBuilder struct {
	released bool
	builder  *array.StructBuilder
	scb      *array.Int32Builder            // status code builder
	smb      *array.BinaryDictionaryBuilder // status message builder
}

func NewStatusBuilder(pool *memory.GoAllocator) *StatusBuilder {
	return StatusBuilderFrom(array.NewStructBuilder(pool, StatusDT))
}

func StatusBuilderFrom(sb *array.StructBuilder) *StatusBuilder {
	return &StatusBuilder{
		released: false,
		builder:  sb,
		scb:      sb.FieldBuilder(0).(*array.Int32Builder),
		smb:      sb.FieldBuilder(1).(*array.BinaryDictionaryBuilder),
	}
}

// Append appends a new span status to the builder.
//
// This method panics if the builder has already been released.
func (b *StatusBuilder) Append(status ptrace.SpanStatus) error {
	if b.released {
		panic("status builder already released")
	}

	b.builder.Append(true)
	b.scb.Append(int32(status.Code()))
	message := status.Message()
	if message == "" {
		b.smb.AppendNull()
	} else {
		if err := b.smb.AppendString(message); err != nil {
			return err
		}
	}
	return nil
}

// Build builds the span status array struct.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *StatusBuilder) Build() *array.Struct {
	if b.released {
		panic("status builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray()
}

// Release releases the memory allocated by the builder.
func (b *StatusBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.scb.Release()
		b.smb.Release()

		b.released = true
	}
}
