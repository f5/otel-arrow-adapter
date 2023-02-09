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

// Uint8Builder is a wrapper around the arrow array builder for uint8.
type Uint8Builder struct {
	builder       *array.Uint8Builder
	transformNode *schema.TransformNode
	updateRequest *SchemaUpdateRequest
}

func (b *Uint8Builder) Append(value uint8) {
	if b.builder != nil {
		b.builder.Append(value)
		return
	}

	// If the builder is nil, then the transform node is not optional.
	b.transformNode.RemoveOptional()
	b.updateRequest.count++
}

func (b *Uint8Builder) AppendNonZero(value uint8) {
	if b.builder != nil {
		if value != 0 {
			b.builder.Append(value)
		} else {
			b.builder.AppendNull()
		}
		return
	}

	// If the builder is nil, then the transform node is not optional.
	b.transformNode.RemoveOptional()
	b.updateRequest.count++
}

// Uint64Builder is a wrapper around the arrow array builder for uint64.
type Uint64Builder struct {
	builder       *array.Uint64Builder
	transformNode *schema.TransformNode
	updateRequest *SchemaUpdateRequest
}

func (b *Uint64Builder) Append(value uint64) {
	if b.builder != nil {
		b.builder.Append(value)
		return
	}

	// If the builder is nil, then the transform node is not optional.
	b.transformNode.RemoveOptional()
	b.updateRequest.count++
}

func (b *Uint64Builder) AppendNonZero(value uint64) {
	if b.builder != nil {
		if value != 0 {
			b.builder.Append(value)
		} else {
			b.builder.AppendNull()
		}
		return
	}

	// If the builder is nil, then the transform node is not optional.
	b.transformNode.RemoveOptional()
	b.updateRequest.count++
}
