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

package common

import (
	"fmt"

	"github.com/apache/arrow/go/v9/arrow"

	"otel-arrow-adapter/pkg/air"
	"otel-arrow-adapter/pkg/air/config"
	"otel-arrow-adapter/pkg/air/rfield"
	"otel-arrow-adapter/pkg/otel/constants"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func NewAttributes(attributes pcommon.Map, cfg *config.Config) *rfield.Field {
	switch cfg.Attribute.Encoding {
	case config.AttributesAsStructs:
		return NewAttributesAsStructs(attributes)
	case config.AttributesAsLists:
		return NewAttributesAsLists(attributes)
	case config.AttributesAsListStructs:
		return NewAttributesAsListStructs(attributes)
	default:
		panic(fmt.Sprintf("unknown attribute encoding: %s", cfg.Attribute.Encoding))
	}
}

func NewAttributesAsStructs(attributes pcommon.Map) *rfield.Field {
	if attributes == nil || len(attributes) == 0 {
		return nil
	}

	attributeFields := make([]*rfield.Field, 0, len(attributes))

	for _, attribute := range attributes {
		value := OtlpAnyValueToValue(attribute.Value)
		if value != nil {
			attributeFields = append(attributeFields, &rfield.Field{
				Name:  attribute.Key,
				Value: value,
			})
		}
	}
	if len(attributeFields) > 0 {
		attrs := rfield.NewStructField(constants.ATTRIBUTES, rfield.Struct{
			Fields: attributeFields,
		})
		return attrs
	}
	return nil
}

func NewAttributesAsLists(attributes pcommon.Map) *rfield.Field {
	if attributes == nil || len(attributes) == 0 {
		return nil
	}

	attributeFields := make([]*rfield.Field, 0, len(attributes))

	stringAttrKeys := make([]rfield.Value, 0, len(attributes))
	stringAttrValues := make([]rfield.Value, 0, len(attributes))

	i64AttrKeys := make([]rfield.Value, 0, len(attributes))
	i64AttrValues := make([]rfield.Value, 0, len(attributes))

	f64AttrKeys := make([]rfield.Value, 0, len(attributes))
	f64AttrValues := make([]rfield.Value, 0, len(attributes))

	boolAttrKeys := make([]rfield.Value, 0, len(attributes))
	boolAttrValues := make([]rfield.Value, 0, len(attributes))

	for _, attribute := range attributes {
		value := OtlpAnyValueToValue(attribute.Value)
		if value != nil {
			switch v := value.(type) {
			case *rfield.String:
				stringAttrKeys = append(stringAttrKeys, rfield.NewString(attribute.Key))
				stringAttrValues = append(stringAttrValues, &rfield.String{Value: v.Value})
			case *rfield.I64:
				i64AttrKeys = append(i64AttrKeys, rfield.NewString(attribute.Key))
				i64AttrValues = append(i64AttrValues, &rfield.I64{Value: v.Value})
			case *rfield.F64:
				f64AttrKeys = append(f64AttrKeys, rfield.NewString(attribute.Key))
				f64AttrValues = append(f64AttrValues, &rfield.F64{Value: v.Value})
			case *rfield.Bool:
				boolAttrKeys = append(boolAttrKeys, rfield.NewString(attribute.Key))
				boolAttrValues = append(boolAttrValues, &rfield.Bool{Value: v.Value})
			default:
				panic(fmt.Sprintf("unsupported type: %T", value))
				//attributeFields = append(attributeFields, &rfield.Field{
				//	Name:  attribute.Key,
				//	Value: value,
				//})
			}
		}
	}
	if len(stringAttrKeys) > 0 {
		attributeFields = append(attributeFields, rfield.NewListField("string_attr_keys", rfield.List{
			Values: stringAttrKeys,
		}))
		attributeFields = append(attributeFields, rfield.NewListField("string_attr_values", rfield.List{
			Values: stringAttrValues,
		}))
	}
	if len(i64AttrKeys) > 0 {
		attributeFields = append(attributeFields, rfield.NewListField("i64_attr_keys", rfield.List{
			Values: i64AttrKeys,
		}))
		attributeFields = append(attributeFields, rfield.NewListField("i64_attr_values", rfield.List{
			Values: i64AttrValues,
		}))
	}
	if len(f64AttrKeys) > 0 {
		attributeFields = append(attributeFields, rfield.NewListField("f64_attr_keys", rfield.List{
			Values: f64AttrKeys,
		}))
		attributeFields = append(attributeFields, rfield.NewListField("f64_attr_values", rfield.List{
			Values: f64AttrValues,
		}))
	}
	if len(boolAttrKeys) > 0 {
		attributeFields = append(attributeFields, rfield.NewListField("bool_attr_keys", rfield.List{
			Values: boolAttrKeys,
		}))
		attributeFields = append(attributeFields, rfield.NewListField("bool_attr_values", rfield.List{
			Values: boolAttrValues,
		}))
	}
	if len(attributeFields) > 0 {
		attrs := rfield.NewStructField(constants.ATTRIBUTES, rfield.Struct{
			Fields: attributeFields,
		})
		return attrs
	}
	return nil
}

type AttributeTuple struct {
	key    string
	i64    *int64
	f64    *float64
	str    *string
	bool   *bool
	binary []byte
}

type AttributeTuples []AttributeTuple

// Sort interface
func (f AttributeTuples) Less(i, j int) bool {
	return f[i].key < f[j].key
}
func (f AttributeTuples) Len() int      { return len(f) }
func (f AttributeTuples) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

func NewAttributesAsListStructs(attributes pcommon.Map) *rfield.Field {
	if attributes == nil || len(attributes) == 0 {
		return nil
	}

	attributeTuples := make([]AttributeTuple, 0, len(attributes))

	stringCount := 0
	i64Count := 0
	f64Count := 0
	boolCount := 0
	binaryCount := 0

	for _, attribute := range attributes {
		value := OtlpAnyValueToValue(attribute.Value)
		if value != nil {
			switch v := value.(type) {
			case *rfield.String:
				attributeTuples = append(attributeTuples, AttributeTuple{
					key: attribute.Key,
					str: v.Value,
				})
				stringCount++
			case *rfield.I64:
				attributeTuples = append(attributeTuples, AttributeTuple{
					key: attribute.Key,
					i64: v.Value,
				})
				i64Count++
			case *rfield.F64:
				attributeTuples = append(attributeTuples, AttributeTuple{
					key: attribute.Key,
					f64: v.Value,
				})
				f64Count++
			case *rfield.Bool:
				attributeTuples = append(attributeTuples, AttributeTuple{
					key:  attribute.Key,
					bool: v.Value,
				})
				boolCount++
			case *rfield.Binary:
				attributeTuples = append(attributeTuples, AttributeTuple{
					key:    attribute.Key,
					binary: v.Value,
				})
				binaryCount++
			default:
				panic(fmt.Sprintf("unexpected type: %T", v))
			}
		}
	}
	values := make([]rfield.Value, 0, len(attributes))
	if len(attributeTuples) > 0 {
		// ToDo remove attribute tuples and create the structs directly as the sorting of attributeTuples doesn't improve compression ratio
		for _, attribute := range attributeTuples {
			fields := make([]*rfield.Field, 0, len(attributeTuples))
			fields = append(fields, &rfield.Field{Name: "key", Value: &rfield.String{Value: &attribute.key}})
			if stringCount > 0 {
				fields = append(fields, &rfield.Field{Name: "string", Value: &rfield.String{Value: attribute.str}})
			}
			if i64Count > 0 {
				fields = append(fields, &rfield.Field{Name: "i64", Value: &rfield.I64{Value: attribute.i64}})
			}
			if f64Count > 0 {
				fields = append(fields, &rfield.Field{Name: "f64", Value: &rfield.F64{Value: attribute.f64}})
			}
			if boolCount > 0 {
				fields = append(fields, &rfield.Field{Name: "bool", Value: &rfield.Bool{Value: attribute.bool}})
			}
			if binaryCount > 0 {
				fields = append(fields, &rfield.Field{Name: "binary", Value: &rfield.Binary{Value: attribute.binary}})
			}
			values = append(values, &rfield.Struct{Fields: fields})
		}
	}

	if len(values) > 0 {
		fieldStruct := make([]arrow.Field, 0, 6)
		if binaryCount > 0 {
			fieldStruct = append(fieldStruct, arrow.Field{Name: "binary", Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: arrow.Metadata{}})
		}
		if boolCount > 0 {
			fieldStruct = append(fieldStruct, arrow.Field{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true, Metadata: arrow.Metadata{}})
		}
		if f64Count > 0 {
			fieldStruct = append(fieldStruct, arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true, Metadata: arrow.Metadata{}})
		}
		if i64Count > 0 {
			fieldStruct = append(fieldStruct, arrow.Field{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: arrow.Metadata{}})
		}
		fieldStruct = append(fieldStruct, arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.Metadata{}})
		if stringCount > 0 {
			fieldStruct = append(fieldStruct, arrow.Field{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.Metadata{}})
		}
		etype := arrow.StructOf(fieldStruct...)
		attrs := rfield.NewListField(constants.ATTRIBUTES, *rfield.UnsafeNewList(etype, values))
		return attrs
	}
	return nil
}

func AttributesValue(attributes pcommon.Map) rfield.Value {
	if attributes == nil || len(attributes) == 0 {
		return nil
	}

	attributeFields := make([]*rfield.Field, 0, len(attributes))

	for _, attribute := range attributes {
		value := OtlpAnyValueToValue(attribute.Value)
		if value != nil {
			attributeFields = append(attributeFields, &rfield.Field{
				Name:  attribute.Key,
				Value: value,
			})
		}
	}
	if len(attributeFields) > 0 {
		return &rfield.Struct{
			Fields: attributeFields,
		}
	}
	return nil
}

func AddResource(record *air.Record, resource pcommon.Resource, cfg *config.Config) {
	resourceField := ResourceField(resource, cfg)
	if resourceField != nil {
		record.AddField(resourceField)
	}
}

func ResourceField(resource pcommon.Resource, cfg *config.Config) *rfield.Field {
	var resourceFields []*rfield.Field

	// ToDo test attributes := NewAttributes(resource.Attributes)
	attributes := NewAttributes(resource.Attributes, cfg)
	if attributes != nil {
		resourceFields = append(resourceFields, attributes)
	}

	if resource.DroppedAttributesCount > 0 {
		resourceFields = append(resourceFields, rfield.NewU32Field(constants.DROPPED_ATTRIBUTES_COUNT, resource.DroppedAttributesCount))
	}
	if len(resourceFields) > 0 {
		field := rfield.NewStructField(constants.RESOURCE, rfield.Struct{
			Fields: resourceFields,
		})
		return field
	} else {
		return nil
	}
}

func AddScope(record *air.Record, scopeKey string, scope pcommon.InstrumentationScope, cfg *config.Config) {
	scopeField := ScopeField(scopeKey, scope, cfg)
	if scopeField != nil {
		record.AddField(scopeField)
	}
}

func ScopeField(scopeKey string, scope pcommon.InstrumentationScope, cfg *config.Config) *rfield.Field {
	var fields []*rfield.Field

	if len(scope.Name) > 0 {
		fields = append(fields, rfield.NewStringField(constants.NAME, scope.Name))
	}
	if len(scope.Version) > 0 {
		fields = append(fields, rfield.NewStringField(constants.VERSION, scope.Version))
	}
	// todo attributes := NewAttributes(scope.Attributes)
	attributes := NewAttributes(scope.Attributes, cfg)
	if attributes != nil {
		fields = append(fields, attributes)
	}
	if scope.DroppedAttributesCount > 0 {
		fields = append(fields, rfield.NewU32Field(constants.DROPPED_ATTRIBUTES_COUNT, scope.DroppedAttributesCount))
	}

	field := rfield.NewStructField(scopeKey, rfield.Struct{
		Fields: fields,
	})
	return field
}

func OtlpAnyValueToValue(value pcommon.Value) rfield.Value {
	if value != nil {
		switch value.Value.(type) {
		case pcommon.Value_BoolValue:
			return rfield.NewBool(value.GetBoolValue())
		case pcommon.Value_IntValue:
			return rfield.NewI64(value.GetIntValue())
		case pcommon.Value_DoubleValue:
			return rfield.NewF64(value.GetDoubleValue())
		case pcommon.Value_StringValue:
			return rfield.NewString(value.GetStringValue())
		case pcommon.Value_BytesValue:
			return &rfield.Binary{Value: value.GetBytesValue()}
		case pcommon.Value_ArrayValue:
			values := value.GetArrayValue()
			fieldValues := make([]rfield.Value, 0, len(values.Values))
			for _, value := range values.Values {
				v := OtlpAnyValueToValue(value)
				if v != nil {
					fieldValues = append(fieldValues, v)
				}
			}
			if len(fieldValues) > 0 {
				return &rfield.List{Values: fieldValues}
			}
			return nil
		case pcommon.Value_KvlistValue:
			values := value.GetKvlistValue()
			if values == nil || len(values.Values) == 0 {
				return nil
			} else {
				fields := make([]*rfield.Field, 0, len(values.Values))
				for _, kv := range values.Values {
					v := OtlpAnyValueToValue(kv.Value)
					if v != nil {
						fields = append(fields, &rfield.Field{
							Name:  kv.Key,
							Value: v,
						})
					}
				}
				if len(fields) > 0 {
					return &rfield.Struct{Fields: fields}
				}
				return nil
			}
		default:
			return nil
		}
	} else {
		return nil
	}
}
