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

type StringBuilder struct {
	builder       *array.StringBuilder
	transformNode *schema.TransformNode
	updateRequest *SchemaUpdateRequest
}

func (b *StringBuilder) AppendNull() {
	if b.builder != nil {
		b.builder.AppendNull()
		return
	}
}

func (b *StringBuilder) Append(value string) {
	if b.builder != nil {
		if value == "" {
			b.builder.AppendNull()
			return
		}
		b.builder.Append(value)
		return
	}
	b.transformNode.RemoveOptional()
	b.updateRequest.count++
}
