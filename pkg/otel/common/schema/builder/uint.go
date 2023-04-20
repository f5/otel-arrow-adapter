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
	"github.com/apache/arrow/go/v12/arrow/array"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/update"
)

// Uint8Builder is a wrapper around the arrow array builder for uint8.
type Uint8Builder struct {
	builder       array.Builder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

func (b *Uint8Builder) Append(value uint8) {
	if b.builder != nil {
		switch builder := b.builder.(type) {
		case *array.Uint8Builder:
			builder.Append(value)
		case *array.Uint8DictionaryBuilder:
			if err := builder.Append(value); err != nil {
				// Should never happen.
				panic(err)
			}
		default:
			// Should never happen.
			panic("unknown builder type")
		}

		return
	}

	if value != 0 {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc()
	}
}

func (b *Uint8Builder) AppendNonZero(value uint8) {
	if b.builder != nil {
		if value != 0 {
			switch builder := b.builder.(type) {
			case *array.Uint8Builder:
				builder.Append(value)
			case *array.Uint8DictionaryBuilder:
				if err := builder.Append(value); err != nil {
					// Should never happen.
					panic(err)
				}
			default:
				// Should never happen.
				panic("unknown builder type")
			}
		} else {
			b.builder.AppendNull()
		}
		return
	}

	if value != 0 {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc()
	}
}

// Uint16Builder is a wrapper around the arrow array builder for uint16.
type Uint16Builder struct {
	builder       array.Builder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

func (b *Uint16Builder) Append(value uint16) {
	if b.builder != nil {
		switch builder := b.builder.(type) {
		case *array.Uint16Builder:
			builder.Append(value)
		case *array.Uint16DictionaryBuilder:
			if err := builder.Append(value); err != nil {
				// Should never happen.
				panic(err)
			}
		default:
			// Should never happen.
			panic("unknown builder type")
		}
		return
	}

	if value != 0 {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc()
	}
}

func (b *Uint16Builder) AppendNonZero(value uint16) {
	if b.builder != nil {
		if value != 0 {
			switch builder := b.builder.(type) {
			case *array.Uint16Builder:
				builder.Append(value)
			case *array.Uint16DictionaryBuilder:
				if err := builder.Append(value); err != nil {
					// Should never happen.
					panic(err)
				}
			default:
				// Should never happen.
				panic("unknown builder type")
			}
		} else {
			b.builder.AppendNull()
		}
		return
	}

	if value != 0 {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc()
	}
}

func (b *Uint16Builder) AppendNull() {
	if b.builder != nil {
		b.builder.AppendNull()
		return
	}
}

// Uint32Builder is a wrapper around the arrow array builder for uint32.
type Uint32Builder struct {
	builder       array.Builder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

func (b *Uint32Builder) Append(value uint32) {
	if b.builder != nil {
		switch builder := b.builder.(type) {
		case *array.Uint32Builder:
			builder.Append(value)
		case *array.Uint32DictionaryBuilder:
			if err := builder.Append(value); err != nil {
				// Should never happen.
				panic(err)
			}
		default:
			// Should never happen.
			panic("unknown builder type")
		}
		return
	}

	if value != 0 {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc()
	}
}

func (b *Uint32Builder) AppendNonZero(value uint32) {
	if b.builder != nil {
		if value != 0 {
			switch builder := b.builder.(type) {
			case *array.Uint32Builder:
				builder.Append(value)
			case *array.Uint32DictionaryBuilder:
				if err := builder.Append(value); err != nil {
					// Should never happen.
					panic(err)
				}
			default:
				// Should never happen.
				panic("unknown builder type")
			}
		} else {
			b.builder.AppendNull()
		}
		return
	}

	if value != 0 {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc()
	}
}

func (b *Uint32Builder) AppendNull() {
	if b.builder != nil {
		b.builder.AppendNull()
		return
	}
}

// Uint32DeltaBuilder is a wrapper around the arrow array builder for uint32
// with delta encoding.
type Uint32DeltaBuilder struct {
	builder       array.Builder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
	prev          uint32
}

func (b *Uint32DeltaBuilder) Append(value uint32) {
	if b.builder != nil {
		switch builder := b.builder.(type) {
		case *array.Uint32Builder:
			if builder.Len() == 0 {
				builder.Append(value)
			} else {
				if value < b.prev {
					// Should never happen.
					panic("value is less than previous value")
				}
				delta := value - b.prev
				builder.Append(delta)
			}
			b.prev = value
		case *array.Uint32DictionaryBuilder:
			if err := builder.Append(value); err != nil {
				// Should never happen.
				panic(err)
			}
		default:
			// Should never happen.
			panic("unknown builder type")
		}
		return
	}

	if value != 0 {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc()
	}
}

func (b *Uint32DeltaBuilder) AppendNull() {
	if b.builder != nil {
		if b.builder.Len() == 0 {
			b.prev = 0
		}
		b.builder.AppendNull()
		return
	}
}

// Uint64Builder is a wrapper around the arrow array builder for uint64.
type Uint64Builder struct {
	builder       array.Builder
	transformNode *schema.TransformNode
	updateRequest *update.SchemaUpdateRequest
}

func (b *Uint64Builder) Append(value uint64) {
	if b.builder != nil {
		switch builder := b.builder.(type) {
		case *array.Uint64Builder:
			builder.Append(value)
		case *array.Uint64DictionaryBuilder:
			if err := builder.Append(value); err != nil {
				// Should never happen.
				panic(err)
			}
		default:
			// Should never happen.
			panic("unknown builder type")
		}
		return
	}

	if value != 0 {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc()
	}
}

func (b *Uint64Builder) AppendNonZero(value uint64) {
	if b.builder != nil {
		if value != 0 {
			switch builder := b.builder.(type) {
			case *array.Uint64Builder:
				builder.Append(value)
			case *array.Uint64DictionaryBuilder:
				if err := builder.Append(value); err != nil {
					// Should never happen.
					panic(err)
				}
			default:
				// Should never happen.
				panic("unknown builder type")
			}
		} else {
			b.builder.AppendNull()
		}
		return
	}

	if value != 0 {
		// If the builder is nil, then the transform node is not optional.
		b.transformNode.RemoveOptional()
		b.updateRequest.Inc()
	}
}

func (b *Uint64Builder) AppendNull() {
	if b.builder != nil {
		b.builder.AppendNull()
		return
	}
}
