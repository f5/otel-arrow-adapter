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

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

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

// NDP1 returns a pmetric.NumberDataPoint (sample 1).
func NDP1() pmetric.NumberDataPoint {
	dp := pmetric.NewNumberDataPoint()
	Attrs1().CopyTo(dp.Attributes())
	dp.SetStartTimestamp(1)
	dp.SetTimestamp(2)
	dp.SetDoubleValue(1.5)
	exs := dp.Exemplars()
	exs.EnsureCapacity(2)
	Exemplar1().CopyTo(exs.AppendEmpty())
	Exemplar2().CopyTo(exs.AppendEmpty())
	dp.SetFlags(1)
	return dp
}

// NDP2 returns a pmetric.NumberDataPoint (sample 1).
func NDP2() pmetric.NumberDataPoint {
	dp := pmetric.NewNumberDataPoint()
	Attrs2().CopyTo(dp.Attributes())
	dp.SetStartTimestamp(2)
	dp.SetTimestamp(3)
	dp.SetIntValue(2)
	exs := dp.Exemplars()
	exs.EnsureCapacity(1)
	Exemplar2().CopyTo(exs.AppendEmpty())
	dp.SetFlags(2)
	return dp
}

// NDP3 returns a pmetric.NumberDataPoint (sample 1).
func NDP3() pmetric.NumberDataPoint {
	dp := pmetric.NewNumberDataPoint()
	Attrs3().CopyTo(dp.Attributes())
	dp.SetStartTimestamp(3)
	dp.SetTimestamp(4)
	dp.SetIntValue(3)
	exs := dp.Exemplars()
	exs.EnsureCapacity(1)
	Exemplar1().CopyTo(exs.AppendEmpty())
	dp.SetFlags(3)
	return dp
}

func Exemplar1() pmetric.Exemplar {
	ex := pmetric.NewExemplar()
	Attrs1().CopyTo(ex.FilteredAttributes())
	ex.SetTimestamp(1)
	ex.SetDoubleValue(1.5)
	ex.SetSpanID([8]byte{0xAA})
	ex.SetTraceID([16]byte{0xAA})
	return ex
}

func Exemplar2() pmetric.Exemplar {
	ex := pmetric.NewExemplar()
	Attrs2().CopyTo(ex.FilteredAttributes())
	ex.SetTimestamp(2)
	ex.SetIntValue(2)
	ex.SetSpanID([8]byte{0xAA})
	ex.SetTraceID([16]byte{0xAA})
	return ex
}

func Gauge1() pmetric.Gauge {
	g := pmetric.NewGauge()
	NDP1().CopyTo(g.DataPoints().AppendEmpty())
	NDP2().CopyTo(g.DataPoints().AppendEmpty())
	NDP3().CopyTo(g.DataPoints().AppendEmpty())
	return g
}

func Gauge2() pmetric.Gauge {
	g := pmetric.NewGauge()
	NDP3().CopyTo(g.DataPoints().AppendEmpty())
	return g
}

func Sum1() pmetric.Sum {
	g := pmetric.NewSum()
	NDP1().CopyTo(g.DataPoints().AppendEmpty())
	NDP2().CopyTo(g.DataPoints().AppendEmpty())
	NDP3().CopyTo(g.DataPoints().AppendEmpty())
	g.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	g.SetIsMonotonic(true)
	return g
}

func Sum2() pmetric.Sum {
	g := pmetric.NewSum()
	NDP3().CopyTo(g.DataPoints().AppendEmpty())
	g.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	g.SetIsMonotonic(false)
	return g
}

func Summary1() pmetric.Summary {
	s := pmetric.NewSummary()
	SummaryDataPoint1().CopyTo(s.DataPoints().AppendEmpty())
	SummaryDataPoint2().CopyTo(s.DataPoints().AppendEmpty())
	return s
}

func Summary2() pmetric.Summary {
	s := pmetric.NewSummary()
	SummaryDataPoint2().CopyTo(s.DataPoints().AppendEmpty())
	return s
}

func SummaryDataPoint1() pmetric.SummaryDataPoint {
	dp := pmetric.NewSummaryDataPoint()
	Attrs1().CopyTo(dp.Attributes())
	dp.SetStartTimestamp(1)
	dp.SetTimestamp(2)
	dp.SetCount(1)
	dp.SetSum(1.5)
	qvs := dp.QuantileValues()
	qvs.EnsureCapacity(2)
	QuantileValue1().CopyTo(qvs.AppendEmpty())
	QuantileValue2().CopyTo(qvs.AppendEmpty())
	dp.SetFlags(1)
	return dp
}

func SummaryDataPoint2() pmetric.SummaryDataPoint {
	dp := pmetric.NewSummaryDataPoint()
	Attrs2().CopyTo(dp.Attributes())
	dp.SetStartTimestamp(3)
	dp.SetTimestamp(4)
	dp.SetCount(2)
	dp.SetSum(2.5)
	qvs := dp.QuantileValues()
	qvs.EnsureCapacity(1)
	QuantileValue2().CopyTo(qvs.AppendEmpty())
	dp.SetFlags(2)
	return dp
}

func QuantileValue1() pmetric.SummaryDataPointValueAtQuantile {
	qv := pmetric.NewSummaryDataPointValueAtQuantile()
	qv.SetQuantile(0.1)
	qv.SetValue(1.5)
	return qv
}

func QuantileValue2() pmetric.SummaryDataPointValueAtQuantile {
	qv := pmetric.NewSummaryDataPointValueAtQuantile()
	qv.SetQuantile(0.2)
	qv.SetValue(2.5)
	return qv
}
