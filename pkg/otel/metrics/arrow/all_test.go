package arrow

import (
	"testing"

	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/f5/otel-arrow-adapter/pkg/otel/internal"
)

func TestValue(t *testing.T) {
	t.Parallel()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	mvb := NewMetricValueBuilder(pool)

	if err := mvb.AppendNumberDataPointValue(NDP1()); err != nil {
		t.Fatal(err)
	}
	if err := mvb.AppendNumberDataPointValue(NDP2()); err != nil {
		t.Fatal(err)
	}
	if err := mvb.AppendNumberDataPointValue(NDP3()); err != nil {
		t.Fatal(err)
	}
	arr, err := mvb.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[[2,1.5]
,[1,2]
,[1,3]
]`

	require.JSONEq(t, expected, string(json))
}

func TestExemplar(t *testing.T) {
	t.Parallel()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	exb := NewExemplarBuilder(pool)

	if err := exb.Append(Exemplar1()); err != nil {
		t.Fatal(err)
	}
	if err := exb.Append(Exemplar2()); err != nil {
		t.Fatal(err)
	}
	arr, err := exb.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":1,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[2,1.5]}
,{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":2,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[1,2]}
]`

	require.JSONEq(t, expected, string(json))
}

func TestUnivariateNDP(t *testing.T) {
	t.Parallel()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	exb := NewNumberDataPointBuilder(pool)

	if err := exb.Append(NDP1()); err != nil {
		t.Fatal(err)
	}
	if err := exb.Append(NDP2()); err != nil {
		t.Fatal(err)
	}
	if err := exb.Append(NDP3()); err != nil {
		t.Fatal(err)
	}
	arr, err := exb.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":1,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[2,1.5]},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":2,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[1,2]}],"flags":1,"start_time_unix_nano":1,"time_unix_nano":2,"value":[2,1.5]}
,{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":2,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[1,2]}],"flags":2,"start_time_unix_nano":2,"time_unix_nano":3,"value":[1,2]}
,{"attributes":[{"key":"str","value":[0,"string3"]},{"key":"double","value":[2,3]},{"key":"bool","value":[3,false]},{"key":"bytes","value":[4,"Ynl0ZXMz"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":1,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[2,1.5]}],"flags":3,"start_time_unix_nano":3,"time_unix_nano":4,"value":[1,3]}
]`

	require.JSONEq(t, expected, string(json))
}

func TestUnivariateGauge(t *testing.T) {
	t.Parallel()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	gb := NewUnivariateGaugeBuilder(pool)

	if err := gb.Append(Gauge1()); err != nil {
		t.Fatal(err)
	}
	if err := gb.Append(Gauge2()); err != nil {
		t.Fatal(err)
	}
	arr, err := gb.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"data_points":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":1,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[2,1.5]},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":2,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[1,2]}],"flags":1,"start_time_unix_nano":1,"time_unix_nano":2,"value":[2,1.5]},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":2,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[1,2]}],"flags":2,"start_time_unix_nano":2,"time_unix_nano":3,"value":[1,2]},{"attributes":[{"key":"str","value":[0,"string3"]},{"key":"double","value":[2,3]},{"key":"bool","value":[3,false]},{"key":"bytes","value":[4,"Ynl0ZXMz"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":1,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[2,1.5]}],"flags":3,"start_time_unix_nano":3,"time_unix_nano":4,"value":[1,3]}]}
,{"data_points":[{"attributes":[{"key":"str","value":[0,"string3"]},{"key":"double","value":[2,3]},{"key":"bool","value":[3,false]},{"key":"bytes","value":[4,"Ynl0ZXMz"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":1,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[2,1.5]}],"flags":3,"start_time_unix_nano":3,"time_unix_nano":4,"value":[1,3]}]}
]`

	require.JSONEq(t, expected, string(json))
}

func TestUnivariateSum(t *testing.T) {
	t.Parallel()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	sb := NewUnivariateSumBuilder(pool)

	if err := sb.Append(Sum1()); err != nil {
		t.Fatal(err)
	}
	if err := sb.Append(Sum2()); err != nil {
		t.Fatal(err)
	}
	arr, err := sb.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"aggregation_temporality":1,"data_points":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":1,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[2,1.5]},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":2,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[1,2]}],"flags":1,"start_time_unix_nano":1,"time_unix_nano":2,"value":[2,1.5]},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":2,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[1,2]}],"flags":2,"start_time_unix_nano":2,"time_unix_nano":3,"value":[1,2]},{"attributes":[{"key":"str","value":[0,"string3"]},{"key":"double","value":[2,3]},{"key":"bool","value":[3,false]},{"key":"bytes","value":[4,"Ynl0ZXMz"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":1,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[2,1.5]}],"flags":3,"start_time_unix_nano":3,"time_unix_nano":4,"value":[1,3]}],"is_monotonic":true}
,{"aggregation_temporality":2,"data_points":[{"attributes":[{"key":"str","value":[0,"string3"]},{"key":"double","value":[2,3]},{"key":"bool","value":[3,false]},{"key":"bytes","value":[4,"Ynl0ZXMz"]}],"exemplars":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"span_id":"qgAAAAAAAAA=","time_unix_nano":1,"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","value":[2,1.5]}],"flags":3,"start_time_unix_nano":3,"time_unix_nano":4,"value":[1,3]}],"is_monotonic":false}
]`

	require.JSONEq(t, expected, string(json))
}

func TestQuantileValue(t *testing.T) {
	t.Parallel()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	sb := NewQuantileValueBuilder(pool)

	if err := sb.Append(QuantileValue1()); err != nil {
		t.Fatal(err)
	}
	if err := sb.Append(QuantileValue2()); err != nil {
		t.Fatal(err)
	}
	arr, err := sb.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"quantile":0.1,"value":1.5}
,{"quantile":0.2,"value":2.5}
]`

	require.JSONEq(t, expected, string(json))
}

// NDP1 returns a pmetric.NumberDataPoint (sample 1).
func NDP1() pmetric.NumberDataPoint {
	dp := pmetric.NewNumberDataPoint()
	internal.Attrs1().CopyTo(dp.Attributes())
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
	internal.Attrs2().CopyTo(dp.Attributes())
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
	internal.Attrs3().CopyTo(dp.Attributes())
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
	internal.Attrs1().CopyTo(ex.FilteredAttributes())
	ex.SetTimestamp(1)
	ex.SetDoubleValue(1.5)
	ex.SetSpanID([8]byte{0xAA})
	ex.SetTraceID([16]byte{0xAA})
	return ex
}

func Exemplar2() pmetric.Exemplar {
	ex := pmetric.NewExemplar()
	internal.Attrs2().CopyTo(ex.FilteredAttributes())
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
