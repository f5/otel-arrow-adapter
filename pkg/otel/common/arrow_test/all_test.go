package arrow_test

import (
	"testing"

	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
)

func TestAttributes(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()
	ab := acommon.NewAttributesBuilder(pool)

	ab.Append(Attrs1())
	ab.Append(Attrs2())
	ab.Append(Attrs3())
	arr := ab.Build()
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}]
,[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}]
,[{"key":"str","value":[0,"string3"]},{"key":"double","value":[2,3]},{"key":"bool","value":[3,false]},{"key":"bytes","value":[4,"Ynl0ZXMz"]}]
]`

	require.JSONEq(t, expected, string(json))
}

func TestScope(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()
	sb := acommon.NewScopeBuilder(pool)

	sb.Append(Scope1())
	sb.Append(Scope2())
	arr := sb.Build()
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"dropped_attributes_count":0,"name":"scope1","version":"1.0.1"}
,{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"dropped_attributes_count":1,"name":"scope2","version":"1.0.2"}
]`

	require.JSONEq(t, expected, string(json))
}

func TestResource(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()
	rb := acommon.NewResourceBuilder(pool)

	rb.Append(Resource1())
	rb.Append(Resource2())
	arr := rb.Build()
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"dropped_attributes_count":0}
,{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"dropped_attributes_count":1}
]`

	require.JSONEq(t, expected, string(json))
}

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
