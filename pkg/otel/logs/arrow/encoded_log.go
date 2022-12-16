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
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
)

var (
	// EncodedLogDT
	EncodedLogDT = arrow.StructOf([]arrow.Field{
		{Name: "log_type", Type: acommon.DefaultDictBinary},
		{Name: "str_vars", Type: arrow.ListOf(acommon.DefaultDictBinary)},
		{Name: "i64_vars", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{Name: "f64_vars", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64)},
	}...)
)

// EncodedLogBuilder is a helper to build an Arrow array containing a collection of encoded logs.
type EncodedLogBuilder struct {
	released bool

	builder *array.StructBuilder // encoded log value builder

	logTypeBuilder *acommon.AdaptiveDictionaryBuilder // log type builder
	svlb           *array.ListBuilder                 // str variable lists builder
	svsb           *acommon.AdaptiveDictionaryBuilder // str variable builder
	ivlb           *array.ListBuilder                 // i64 variable lists builder
	ivsb           *array.Int64Builder                // i64 variable builder
	fvlb           *array.ListBuilder                 // f64 variable lists builder
	fvsb           *array.Float64Builder              // f64 variable builder

	logCompressor *LogCompressor
}

// EncodedLogBuilderFrom creates a new EncodedLogBuilderFrom from an existing StructBuilder.
func EncodedLogBuilderFrom(elb *array.StructBuilder, cfg *common.LogConfig) *EncodedLogBuilder {
	return &EncodedLogBuilder{
		released:       false,
		builder:        elb,
		logTypeBuilder: acommon.AdaptiveDictionaryBuilderFrom(elb.FieldBuilder(0)),
		svlb:           elb.FieldBuilder(1).(*array.ListBuilder),
		svsb:           acommon.AdaptiveDictionaryBuilderFrom(elb.FieldBuilder(1).(*array.ListBuilder).ValueBuilder()),
		ivlb:           elb.FieldBuilder(2).(*array.ListBuilder),
		ivsb:           elb.FieldBuilder(2).(*array.ListBuilder).ValueBuilder().(*array.Int64Builder),
		fvlb:           elb.FieldBuilder(3).(*array.ListBuilder),
		fvsb:           elb.FieldBuilder(3).(*array.ListBuilder).ValueBuilder().(*array.Float64Builder),
		logCompressor:  NewLogCompressor(cfg),
	}
}

// Build builds the "encoded log" Arrow array.
//
// Once the returned array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *EncodedLogBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("encoded log builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

func (b *EncodedLogBuilder) AppendNull() (err error) {
	if b.released {
		err = fmt.Errorf("encoded log builder already released")
		return
	}

	b.builder.AppendNull()
	return
}

// Append appends a new any value to the builder.
func (b *EncodedLogBuilder) Append(log string) (err error) {
	if b.released {
		return fmt.Errorf("encoded log builder already released")
	}

	encodedLog := b.logCompressor.Compress(log)
	if len(encodedLog.LogType) == 0 {
		b.logTypeBuilder.AppendNull()
	} else {
		err = b.logTypeBuilder.AppendString(encodedLog.LogType)
		if err != nil {
			return
		}
	}

	if len(encodedLog.DictVars) == 0 {
		b.svlb.AppendNull()
	} else {
		b.svlb.Append(true)
		b.svlb.Reserve(len(encodedLog.DictVars))
		for _, v := range encodedLog.DictVars {
			err = b.svsb.AppendString(v)
			if err != nil {
				return
			}
		}
	}

	// ToDo - add i64 and f64 vars
	b.ivlb.AppendNull()
	b.fvlb.AppendNull()

	return err
}

// Release releases the memory allocated by the builder.
func (b *EncodedLogBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
