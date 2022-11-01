package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/plog"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// Constants used to identify the type of value in the union.
const (
	StrCode    int8 = 0
	I64Code    int8 = 1
	F64Code    int8 = 2
	BoolCode   int8 = 3
	BinaryCode int8 = 4
	// Future extension CborCode   int8 = 5
)

// Arrow Data Types describing log record and body.
var (
	// LogRecordDT is the Arrow Data Type describing a log record.
	LogRecordDT = arrow.StructOf([]arrow.Field{
		{Name: constants.TIME_UNIX_NANO, Type: arrow.PrimitiveTypes.Uint64},
		{Name: constants.OBSERVED_TIME_UNIX_NANO, Type: arrow.PrimitiveTypes.Uint64},
		{Name: constants.TRACE_ID, Type: acommon.DictU16Fixed16Binary},
		{Name: constants.SPAN_ID, Type: acommon.DictU16Fixed8Binary},
		{Name: constants.SEVERITY_NUMBER, Type: arrow.PrimitiveTypes.Int32},
		{Name: constants.SEVERITY_TEXT, Type: acommon.DictU16String},
		{Name: constants.BODY, Type: BodyDT},
		{Name: constants.ATTRIBUTES, Type: acommon.AttributesDT},
		{Name: constants.DROPPED_ATTRIBUTES_COUNT, Type: arrow.PrimitiveTypes.Uint32},
		{Name: constants.FLAGS, Type: arrow.PrimitiveTypes.Uint32},
	}...)

	// BodyDT is the Arrow Data Type describing a log record body.
	BodyDT = arrow.SparseUnionOf([]arrow.Field{
		// TODO manage case where the cardinality of the dictionary is too high (> 2^16).
		{Name: "str", Type: acommon.DictU16String},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		// TODO manage case where the cardinality of the dictionary is too high (> 2^16).
		{Name: "binary", Type: acommon.DictU16Binary},
		// Future extension {Name: "cbor", Type: acommon.DictU16Binary},
	}, []int8{
		StrCode,
		I64Code,
		F64Code,
		BoolCode,
		BinaryCode,
		// Futuee extension CborCode,
	})
)

// LogRecordBuilder is a helper to build a log record.
type LogRecordBuilder struct {
	released bool

	builder *array.StructBuilder

	tunb  *array.Uint64Builder                    // time unix nano builder
	otunb *array.Uint64Builder                    // observed time unix nano builder
	tib   *array.FixedSizeBinaryDictionaryBuilder // trace id builder
	sib   *array.FixedSizeBinaryDictionaryBuilder // span id builder
	snb   *array.Int32Builder                     // severity number builder
	stb   *array.BinaryDictionaryBuilder          // severity text builder
	bb    *acommon.AnyValueBuilder                // body builder (LOL)
	ab    *acommon.AttributesBuilder              // attributes builder
	dacb  *array.Uint32Builder                    // dropped attributes count builder
	fb    *array.Uint32Builder                    // flags builder
}

// NewLogRecordBuilder creates a new LogRecordBuilder with a given allocator.
//
// Once the builder is no longer needed, Release() must be called to free the
// memory allocated by the builder.
func NewLogRecordBuilder(pool *memory.GoAllocator) *LogRecordBuilder {
	sb := array.NewStructBuilder(pool, LogRecordDT)
	return LogRecordBuilderFrom(sb)
}

func LogRecordBuilderFrom(sb *array.StructBuilder) *LogRecordBuilder {
	return &LogRecordBuilder{
		released: false,
		builder:  sb,
		tunb:     sb.FieldBuilder(0).(*array.Uint64Builder),
		otunb:    sb.FieldBuilder(1).(*array.Uint64Builder),
		tib:      sb.FieldBuilder(2).(*array.FixedSizeBinaryDictionaryBuilder),
		sib:      sb.FieldBuilder(3).(*array.FixedSizeBinaryDictionaryBuilder),
		snb:      sb.FieldBuilder(4).(*array.Int32Builder),
		stb:      sb.FieldBuilder(5).(*array.BinaryDictionaryBuilder),
		bb:       acommon.AnyValueBuilderFrom(sb.FieldBuilder(6).(*array.SparseUnionBuilder)),
		ab:       acommon.AttributesBuilderFrom(sb.FieldBuilder(7).(*array.MapBuilder)),
		dacb:     sb.FieldBuilder(8).(*array.Uint32Builder),
		fb:       sb.FieldBuilder(9).(*array.Uint32Builder),
	}
}

// Build builds the log record array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *LogRecordBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("log builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Append appends a new log record to the builder.
func (b *LogRecordBuilder) Append(log plog.LogRecord) error {
	if b.released {
		return fmt.Errorf("log record builder already released")
	}

	//b.builder.Append(true)
	//b.stunb.Append(uint64(log.StartTimestamp()))
	//b.etunb.Append(uint64(log.EndTimestamp()))
	//tib := log.TraceID()
	//if err := b.tib.Append(tib[:]); err != nil {
	//	return err
	//}
	//sib := log.SpanID()
	//if err := b.sib.Append(sib[:]); err != nil {
	//	return err
	//}
	//traceState := log.TraceState().AsRaw()
	//if traceState == "" {
	//	b.tsb.AppendNull()
	//} else {
	//	if err := b.tsb.AppendString(traceState); err != nil {
	//		return err
	//	}
	//}
	//psib := log.ParentSpanID()
	//if err := b.psib.Append(psib[:]); err != nil {
	//	return err
	//}
	//name := log.Name()
	//if name == "" {
	//	b.nb.AppendNull()
	//} else {
	//	if err := b.nb.AppendString(name); err != nil {
	//		return err
	//	}
	//}
	//b.kb.Append(int32(log.Kind()))
	//if err := b.ab.Append(log.Attributes()); err != nil {
	//	return err
	//}
	//b.dacb.Append(log.DroppedAttributesCount())
	//evts := log.Events()
	//sc := evts.Len()
	//if sc > 0 {
	//	b.sesb.Append(true)
	//	b.sesb.Reserve(sc)
	//	for i := 0; i < sc; i++ {
	//		if err := b.seb.Append(evts.At(i)); err != nil {
	//			return err
	//		}
	//	}
	//} else {
	//	b.sesb.Append(false)
	//}
	//b.decb.Append(log.DroppedEventsCount())
	//lks := log.Links()
	//lc := lks.Len()
	//if lc > 0 {
	//	b.slsb.Append(true)
	//	b.slsb.Reserve(lc)
	//	for i := 0; i < lc; i++ {
	//		if err := b.slb.Append(lks.At(i)); err != nil {
	//			return err
	//		}
	//	}
	//} else {
	//	b.slsb.Append(false)
	//}
	//b.dlcb.Append(log.DroppedLinksCount())
	//if err := b.sb.Append(log.Status()); err != nil {
	//	return err
	//}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *LogRecordBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.tunb.Release()
		b.otunb.Release()
		b.tib.Release()
		b.sib.Release()
		b.snb.Release()
		b.stb.Release()
		b.bb.Release()
		b.ab.Release()
		b.dacb.Release()
		b.fb.Release()

		b.released = true
	}
}
