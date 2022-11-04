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

	if err := mvb.Append(NDP1()); err != nil {
		t.Fatal(err)
	}
	if err := mvb.Append(NDP2()); err != nil {
		t.Fatal(err)
	}
	if err := mvb.Append(NDP3()); err != nil {
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
	ex.SetDoubleValue(1.0)
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
