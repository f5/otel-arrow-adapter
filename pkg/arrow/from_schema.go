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

// Utility functions to extract ids from Arrow schemas.

import (
	"errors"

	"github.com/apache/arrow/go/v11/arrow"

	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

var (
	ErrDuplicateFieldName = errors.New("duplicate field name")
	ErrNotListOfStruct    = errors.New("not a list of structs")
	ErrNotList            = errors.New("not a list")
)

// ListOfStructsFieldIDFromSchema returns the field id of a list of structs
// field from an Arrow schema or -1 for an unknown field.
//
// An error is returned if the field is not a list of structs.
func ListOfStructsFieldIDFromSchema(schema *arrow.Schema, fieldName string) (int, *arrow.StructType, error) {
	ids := schema.FieldIndices(fieldName)
	if len(ids) == 0 {
		return -1, nil, nil
	}
	if len(ids) > 1 {
		return 0, nil, werror.WrapWithContext(ErrDuplicateFieldName, map[string]interface{}{"fieldName": fieldName})
	}

	if lt, ok := schema.Field(ids[0]).Type.(*arrow.ListType); ok {
		st, ok := lt.ElemField().Type.(*arrow.StructType)
		if !ok {
			return 0, nil, werror.WrapWithContext(ErrNotListOfStruct, map[string]interface{}{"fieldName": fieldName})
		}
		return ids[0], st, nil
	} else {
		return 0, nil, werror.WrapWithContext(ErrNotList, map[string]interface{}{"fieldName": fieldName})
	}
}
