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

package builder

import (
	"github.com/apache/arrow/go/v11/arrow/array"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
)

// BooleanBuilder is a wrapper around the arrow BooleanBuilder.
type BooleanBuilder struct {
	builder       *array.BooleanBuilder
	transformNode *schema.TransformNode
	updateRequest *SchemaUpdateRequest
}

// Append appends a value to the underlying builder and updates the
// transform node if the builder is nil.
func (b *BooleanBuilder) Append(value bool) {
	if b.builder != nil {
		b.builder.Append(value)
		return
	}

	// If the builder is nil, then the transform node is not optional.
	b.transformNode.RemoveOptional()
	b.updateRequest.count++
}

// AppendNull appends a null value to the underlying builder. If the builder is
// nil we do nothing as we have no information about the presence of this field
// in the data.
func (b *BooleanBuilder) AppendNull() {
	if b.builder != nil {
		b.builder.AppendNull()
		return
	}
}
