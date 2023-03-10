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

package internal

import "go.opentelemetry.io/collector/pdata/pcommon"

func Attrs1() pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutStr("str", "string1")
	attrs.PutInt("int", 1)
	attrs.PutDouble("double", 1.0)
	attrs.PutBool("bool", true)
	bytes := attrs.PutEmptyBytes("bytes")
	bytes.Append([]byte("bytes1")...)
	return attrs
}

func Attrs2() pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutStr("str", "string2")
	attrs.PutInt("int", 2)
	attrs.PutDouble("double", 2.0)
	bytes := attrs.PutEmptyBytes("bytes")
	bytes.Append([]byte("bytes2")...)
	return attrs
}

func Attrs3() pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutStr("str", "string3")
	attrs.PutDouble("double", 3.0)
	attrs.PutBool("bool", false)
	bytes := attrs.PutEmptyBytes("bytes")
	bytes.Append([]byte("bytes3")...)
	return attrs
}

func Attrs4() pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutBool("bool", true)
	bytes := attrs.PutEmptyBytes("bytes")
	bytes.Append([]byte("bytes4")...)
	return attrs
}

func Attrs5() pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutBool("attr1", true)
	bytes := attrs.PutEmptyBytes("attr2")
	bytes.Append([]byte("bytes4")...)
	attrs.PutStr("attr3", "string5")
	attrs.PutInt("attr4", 5)
	attrs.PutDouble("attr5", 5.0)
	attrs.PutBool("attr6", false)
	bytes = attrs.PutEmptyBytes("attr7")
	bytes.Append([]byte("bytes5")...)
	attrs.PutStr("attr8", "string6")
	attrs.PutInt("attr9", 6)
	attrs.PutDouble("attr10", 6.0)
	attrs.PutBool("attr11", true)
	bytes = attrs.PutEmptyBytes("attr12")
	bytes.Append([]byte("bytes6")...)
	attrs.PutStr("attr13", "string7")
	return attrs
}

func Scope1() pcommon.InstrumentationScope {
	scope := pcommon.NewInstrumentationScope()
	scope.SetName("scope1")
	scope.SetVersion("1.0.1")
	scopeAttrs := scope.Attributes()
	Attrs1().CopyTo(scopeAttrs)
	scope.SetDroppedAttributesCount(0)
	return scope
}

func Scope2() pcommon.InstrumentationScope {
	scope := pcommon.NewInstrumentationScope()
	scope.SetName("scope2")
	scope.SetVersion("1.0.2")
	scopeAttrs := scope.Attributes()
	Attrs2().CopyTo(scopeAttrs)
	scope.SetDroppedAttributesCount(1)
	return scope
}

func Resource1() pcommon.Resource {
	resource := pcommon.NewResource()
	resourceAttrs := resource.Attributes()
	Attrs1().CopyTo(resourceAttrs)
	resource.SetDroppedAttributesCount(0)
	return resource
}

func Resource2() pcommon.Resource {
	resource := pcommon.NewResource()
	resourceAttrs := resource.Attributes()
	Attrs2().CopyTo(resourceAttrs)
	resource.SetDroppedAttributesCount(1)
	return resource
}
