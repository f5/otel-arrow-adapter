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
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"go.opentelemetry.io/collector/pdata/plog"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

var (
	ResourceLogsDT = arrow.StructOf([]arrow.Field{
		{Name: constants.RESOURCE, Type: acommon.ResourceDT},
		{Name: constants.SCHEMA_URL, Type: acommon.DefaultDictString},
		{Name: constants.SCOPE_LOGS, Type: arrow.ListOf(ScopeLogsDT)},
	}...)
)

// ResourceLogsBuilder is a helper to build resource logs.
type ResourceLogsBuilder struct {
	released bool

	builder *array.StructBuilder // builder for the resource logs struct

	rb   *acommon.ResourceBuilder           // resource builder
	schb *acommon.AdaptiveDictionaryBuilder // schema url builder
	slsb *array.ListBuilder                 // scope logs list builder
	slb  *ScopeLogsBuilder                  // scope logs builder
}

// NewResourceLogsBuilder creates a new ResourceLogsBuilder with a given allocator.
//
// Once the builder is no longer needed, Build() or Release() must be called to free the
// memory allocated by the builder.
func NewResourceLogsBuilder(pool memory.Allocator) *ResourceLogsBuilder {
	builder := array.NewStructBuilder(pool, ResourceLogsDT)
	return ResourceLogsBuilderFrom(builder)
}

// ResourceLogsBuilderFrom creates a new ResourceLogsBuilder from an existing builder.
func ResourceLogsBuilderFrom(builder *array.StructBuilder) *ResourceLogsBuilder {
	return &ResourceLogsBuilder{
		released: false,
		builder:  builder,
		rb:       acommon.ResourceBuilderFrom(builder.FieldBuilder(0).(*array.StructBuilder)),
		schb:     acommon.AdaptiveDictionaryBuilderFrom(builder.FieldBuilder(1)),
		slsb:     builder.FieldBuilder(2).(*array.ListBuilder),
		slb:      ScopeLogsBuilderFrom(builder.FieldBuilder(2).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
	}
}

// Build builds the resource logs array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ResourceLogsBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("resource logs builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Append appends a new resource logs to the builder.
func (b *ResourceLogsBuilder) Append(rs plog.ResourceLogs) error {
	if b.released {
		return fmt.Errorf("resource logs builder already released")
	}

	b.builder.Append(true)
	if err := b.rb.Append(rs.Resource()); err != nil {
		return err
	}
	schemaUrl := rs.SchemaUrl()
	if schemaUrl == "" {
		b.schb.AppendNull()
	} else {
		if err := b.schb.AppendString(schemaUrl); err != nil {
			return err
		}
	}
	slogs := rs.ScopeLogs()
	sc := slogs.Len()
	if sc > 0 {
		b.slsb.Append(true)
		b.slsb.Reserve(sc)
		for i := 0; i < sc; i++ {
			if err := b.slb.Append(slogs.At(i)); err != nil {
				return err
			}
		}
	} else {
		b.slsb.Append(false)
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *ResourceLogsBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
