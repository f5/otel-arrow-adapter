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

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/axiomhq/hyperloglog"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type (
	TraceAnalyzer struct {
		TraceCount         int64
		ResourceSpansStats *ResourceSpansStats
	}

	ResourceSpansStats struct {
		Distribution    *hdrhistogram.Histogram
		ResourceStats   *ResourceStats
		ScopeSpansStats *ScopeSpansStats
		SchemaUrlStats  *SchemaUrlStats
	}

	ScopeSpansStats struct {
		Distribution   *hdrhistogram.Histogram
		ScopeStats     *ScopeStats
		SchemaUrlStats *SchemaUrlStats
		SpanStats      *SpanStats
	}

	SchemaUrlStats struct {
		Missing          int64
		NonEmpty         int64
		SizeDistribution *hdrhistogram.Histogram
	}

	ResourceStats struct {
		TotalCount int64
		Missing    int64

		AttributesStats *AttributesStats
	}

	ScopeStats struct {
		TotalCount int64
		Missing    int64

		AttributesStats *AttributesStats

		Name    *StringStats
		Version *StringStats
	}

	SpanStats struct {
		TotalCount        int64
		Distribution      *hdrhistogram.Histogram
		Attributes        *AttributesStats
		TimeIntervalStats *TimeIntervalStats
		Name              *StringStats
		SpanID            *hyperloglog.Sketch
		TraceID           *hyperloglog.Sketch
		ParentSpanID      *hyperloglog.Sketch
	}

	AttributesStats struct {
		Missing      int64
		Distribution *hdrhistogram.Histogram

		// Attribute type distribution
		I64TypeDistribution    *hdrhistogram.Histogram
		F64TypeDistribution    *hdrhistogram.Histogram
		BoolTypeDistribution   *hdrhistogram.Histogram
		StringTypeDistribution *hdrhistogram.Histogram
		BinaryTypeDistribution *hdrhistogram.Histogram
		ListTypeDistribution   *hdrhistogram.Histogram
		MapTypeDistribution    *hdrhistogram.Histogram

		// Attribute key
		KeyLenDistribution *hdrhistogram.Histogram
		KeyDistinctValue   *hyperloglog.Sketch

		// Attribute distinct values per type
		I64DistinctValue    *hyperloglog.Sketch
		F64DistinctValue    *hyperloglog.Sketch
		StringDistinctValue *hyperloglog.Sketch
		BinaryDistinctValue *hyperloglog.Sketch

		// Content length distribution
		StringLenDistribution *hdrhistogram.Histogram
		BinaryLenDistribution *hdrhistogram.Histogram

		// Dropped Attributes Count distribution
		DACDistribution *hdrhistogram.Histogram
	}

	StringStats struct {
		Missing         int64
		LenDistribution *hdrhistogram.Histogram
		DistinctValue   *hyperloglog.Sketch
	}

	TimeIntervalStats struct {
		TotalCount              int64
		IntervalDistinctValue   *hyperloglog.Sketch
		StartDeltaDistinctValue *hyperloglog.Sketch
		EndDeltaDistinctValue   *hyperloglog.Sketch
	}
)

func NewTraceAnalyzer() *TraceAnalyzer {
	return &TraceAnalyzer{
		ResourceSpansStats: &ResourceSpansStats{
			Distribution: hdrhistogram.New(1, 1000000, 2),
			ResourceStats: &ResourceStats{
				AttributesStats: NewAttributesStats(),
			},
			ScopeSpansStats: &ScopeSpansStats{
				Distribution: hdrhistogram.New(1, 1000000, 2),
				ScopeStats: &ScopeStats{
					AttributesStats: NewAttributesStats(),
					Name:            NewStringStats(),
					Version:         NewStringStats(),
				},
				SpanStats: NewSpanStats(),
				SchemaUrlStats: &SchemaUrlStats{
					SizeDistribution: hdrhistogram.New(1, 10000, 2),
				},
			},
			SchemaUrlStats: &SchemaUrlStats{
				SizeDistribution: hdrhistogram.New(1, 10000, 2),
			},
		},
	}
}

func NewAttributesStats() *AttributesStats {
	return &AttributesStats{
		Distribution: hdrhistogram.New(1, 1000000, 2),

		I64TypeDistribution:    hdrhistogram.New(1, 1000000, 2),
		F64TypeDistribution:    hdrhistogram.New(1, 1000000, 2),
		BoolTypeDistribution:   hdrhistogram.New(1, 1000000, 2),
		StringTypeDistribution: hdrhistogram.New(1, 1000000, 2),
		BinaryTypeDistribution: hdrhistogram.New(1, 1000000, 2),
		ListTypeDistribution:   hdrhistogram.New(1, 1000000, 2),
		MapTypeDistribution:    hdrhistogram.New(1, 1000000, 2),

		KeyLenDistribution: hdrhistogram.New(0, 1000, 2),
		KeyDistinctValue:   hyperloglog.New16(),

		I64DistinctValue:    hyperloglog.New16(),
		F64DistinctValue:    hyperloglog.New16(),
		StringDistinctValue: hyperloglog.New16(),
		BinaryDistinctValue: hyperloglog.New16(),

		StringLenDistribution: hdrhistogram.New(1, 1000000, 2),
		BinaryLenDistribution: hdrhistogram.New(1, 1000000, 2),

		DACDistribution: hdrhistogram.New(1, 1000000, 2),
	}
}

func (t *TraceAnalyzer) Analyze(traces *TracesOptimized) {
	t.TraceCount++
	t.ResourceSpansStats.UpdateWith(traces.ResourceSpans)
}

func (t *TraceAnalyzer) ShowStats(indent string) {
	println()
	fmt.Printf("%sTrace Count: %d\n", indent, t.TraceCount)
	t.ResourceSpansStats.ShowStats(indent)
}

func (r *ResourceSpansStats) UpdateWith(resSpan []*ResourceSpanGroup) {
	RequireNoError(r.Distribution.RecordValue(int64(len(resSpan))))

	for _, rs := range resSpan {
		r.ResourceStats.UpdateWith(rs.Resource)
		r.ScopeSpansStats.UpdateWith(rs.ScopeSpans)
		r.SchemaUrlStats.UpdateWith(rs.ResourceSchemaUrl)
	}
}

func (r *ResourceSpansStats) ShowStats(indent string) {
	fmt.Printf("%sResourceSpans count distribution (total-count, min, max, mean, stdev, p50, p99): %d, %d, %d, %f, %f, %d, %d\n", indent,
		r.Distribution.TotalCount(), r.Distribution.Min(), r.Distribution.Max(), r.Distribution.Mean(), r.Distribution.StdDev(), r.Distribution.ValueAtQuantile(50), r.Distribution.ValueAtQuantile(99),
	)
	r.ResourceStats.ShowStats(indent + "  ")
	r.ScopeSpansStats.ShowStats(indent + "  ")
	r.SchemaUrlStats.ShowStats(indent + "  ")
}

func (s *ScopeStats) UpdateWith(scope *pcommon.InstrumentationScope) {
	if scope == nil {
		s.Missing++
		return
	}

	s.TotalCount++
	s.AttributesStats.UpdateWith(scope.Attributes(), scope.DroppedAttributesCount())
}

func (s *ScopeStats) ShowStats(indent string) {
	fmt.Printf("%sScope distributions (count, missing): %d, %d\n", indent, s.TotalCount, s.Missing)
	indent += "  "
	s.Name.ShowStats("Name", indent)
	s.Version.ShowStats("Version", indent)
	s.AttributesStats.ShowStats(indent)
}

func (s *ScopeSpansStats) UpdateWith(scopeSpans []*ScopeSpanGroup) {
	RequireNoError(s.Distribution.RecordValue(int64(len(scopeSpans))))

	for _, ss := range scopeSpans {
		s.ScopeStats.UpdateWith(ss.Scope)
		s.SpanStats.UpdateWith(ss.Spans)
		s.SchemaUrlStats.UpdateWith(ss.ScopeSchemaUrl)
	}
}

func (s *ScopeSpansStats) ShowStats(indent string) {
	fmt.Printf("%sScopeSpans count distribution (total-count, min, max, mean, stdev, p50, p99): %d, %d, %d, %f, %f, %d, %d\n", indent,
		s.Distribution.TotalCount(), s.Distribution.Min(), s.Distribution.Max(), s.Distribution.Mean(), s.Distribution.StdDev(), s.Distribution.ValueAtQuantile(50), s.Distribution.ValueAtQuantile(99),
	)
	s.ScopeStats.ShowStats(indent + "  ")
	s.SpanStats.ShowStats(indent + "  ")
	s.SchemaUrlStats.ShowStats(indent + "  ")
}

func (r *ResourceStats) UpdateWith(res *pcommon.Resource) {
	attrs := res.Attributes()
	dac := res.DroppedAttributesCount()

	r.TotalCount++

	if attrs.Len() == 0 && dac == 0 {
		r.Missing++
	}

	r.AttributesStats.UpdateWith(attrs, dac)
}

func (r *ResourceStats) ShowStats(indent string) {
	fmt.Printf("%sResource (total-count, missing): %d, %d\n", indent, r.TotalCount, r.Missing)
	r.AttributesStats.ShowStats(indent + "  ")
}

func (a *AttributesStats) UpdateWith(attrs pcommon.Map, dac uint32) {
	if attrs.Len() == 0 {
		a.Missing++
		return
	}

	RequireNoError(a.Distribution.RecordValue(int64(attrs.Len())))

	var (
		i64Count    int64
		f64Count    int64
		boolCount   int64
		stringCount int64
		binaryCount int64
		listCount   int64
		mapCount    int64
	)

	attrs.Range(func(k string, v pcommon.Value) bool {
		RequireNoError(a.KeyLenDistribution.RecordValue(int64(len(k))))
		a.KeyDistinctValue.Insert([]byte(k))

		switch v.Type() {
		case pcommon.ValueTypeInt:
			i64Count++
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(v.Int()))
			a.I64DistinctValue.Insert(b)
		case pcommon.ValueTypeDouble:
			f64Count++
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, math.Float64bits(v.Double()))
			a.F64DistinctValue.Insert(b)
		case pcommon.ValueTypeBool:
			boolCount++
		case pcommon.ValueTypeStr:
			stringCount++
			a.StringDistinctValue.Insert([]byte(v.Str()))
			RequireNoError(a.StringLenDistribution.RecordValue(int64(len(v.Str()))))
		case pcommon.ValueTypeBytes:
			binaryCount++
			a.BinaryDistinctValue.Insert(v.Bytes().AsRaw())
			RequireNoError(a.BinaryLenDistribution.RecordValue(int64(len(v.Bytes().AsRaw()))))
		case pcommon.ValueTypeSlice:
			listCount++
		case pcommon.ValueTypeMap:
			mapCount++
		default:
			// no-op
		}

		return true
	})

	if i64Count > 0 {
		RequireNoError(a.I64TypeDistribution.RecordValue(i64Count))
	}
	if f64Count > 0 {
		RequireNoError(a.F64TypeDistribution.RecordValue(f64Count))
	}
	if boolCount > 0 {
		RequireNoError(a.BoolTypeDistribution.RecordValue(boolCount))
	}
	if stringCount > 0 {
		RequireNoError(a.StringTypeDistribution.RecordValue(stringCount))
	}
	if binaryCount > 0 {
		RequireNoError(a.BinaryTypeDistribution.RecordValue(binaryCount))
	}
	if listCount > 0 {
		RequireNoError(a.ListTypeDistribution.RecordValue(listCount))
	}
	if mapCount > 0 {
		RequireNoError(a.MapTypeDistribution.RecordValue(mapCount))
	}

	RequireNoError(a.DACDistribution.RecordValue(int64(dac)))
}

func (a *AttributesStats) ShowStats(indent string) {
	if a.Distribution.TotalCount() == 0 {
		fmt.Printf("%sNo attributes\n", indent)
		return
	}

	fmt.Printf("%sAttribute distributions\n", indent)
	indent += "  "

	fmt.Printf("%sCount |  count|missing|    min|    max|   mean|  stdev|    p50|    p99|\n", indent)
	fmt.Printf("%s      |%7d|%7d|%7d|%7d|%7.1f|%7.1f|%7d|%7d|\n", indent,
		a.Distribution.TotalCount(), a.Missing, a.Distribution.Min(), a.Distribution.Max(), a.Distribution.Mean(), a.Distribution.StdDev(), a.Distribution.ValueAtQuantile(50), a.Distribution.ValueAtQuantile(99),
	)

	fmt.Printf("%sType     | count|   min|   max|  mean| stdev|   p50|   p99|\n", indent)
	if a.I64TypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sI64    |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent+"  ",
			a.I64TypeDistribution.TotalCount(), a.I64TypeDistribution.Min(), a.I64TypeDistribution.Max(), a.I64TypeDistribution.Mean(), a.I64TypeDistribution.StdDev(), a.I64TypeDistribution.ValueAtQuantile(50), a.I64TypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.F64TypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sF64    |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent+"  ",
			a.F64TypeDistribution.TotalCount(), a.F64TypeDistribution.Min(), a.F64TypeDistribution.Max(), a.F64TypeDistribution.Mean(), a.F64TypeDistribution.StdDev(), a.F64TypeDistribution.ValueAtQuantile(50), a.F64TypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.BoolTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sBool   |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent+"  ",
			a.BoolTypeDistribution.TotalCount(), a.BoolTypeDistribution.Min(), a.BoolTypeDistribution.Max(), a.BoolTypeDistribution.Mean(), a.BoolTypeDistribution.StdDev(), a.BoolTypeDistribution.ValueAtQuantile(50), a.BoolTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.StringTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sString |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent+"  ",
			a.StringTypeDistribution.TotalCount(), a.StringTypeDistribution.Min(), a.StringTypeDistribution.Max(), a.StringTypeDistribution.Mean(), a.StringTypeDistribution.StdDev(), a.StringTypeDistribution.ValueAtQuantile(50), a.StringTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.BinaryTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sBinary |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent+"  ",
			a.BinaryTypeDistribution.TotalCount(), a.BinaryTypeDistribution.Min(), a.BinaryTypeDistribution.Max(), a.BinaryTypeDistribution.Mean(), a.BinaryTypeDistribution.StdDev(), a.BinaryTypeDistribution.ValueAtQuantile(50), a.BinaryTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.ListTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sList   |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent+"  ",
			a.ListTypeDistribution.TotalCount(), a.ListTypeDistribution.Min(), a.ListTypeDistribution.Max(), a.ListTypeDistribution.Mean(), a.ListTypeDistribution.StdDev(), a.ListTypeDistribution.ValueAtQuantile(50), a.ListTypeDistribution.ValueAtQuantile(99),
		)
	}
	if a.MapTypeDistribution.TotalCount() > 0 {
		fmt.Printf("%sMap    |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent+"  ",
			a.MapTypeDistribution.TotalCount(), a.MapTypeDistribution.Min(), a.MapTypeDistribution.Max(), a.MapTypeDistribution.Mean(), a.MapTypeDistribution.StdDev(), a.MapTypeDistribution.ValueAtQuantile(50), a.MapTypeDistribution.ValueAtQuantile(99),
		)
	}

	fmt.Printf("%sKey      |distinct|len-min|len-max|len-mean|len-stdev|len-p50|len-p99|\n", indent)
	fmt.Printf("%s         |%8d|%7d|%7d|%8.2f|%9.2f|%7d|%7d|\n", indent,
		a.KeyDistinctValue.Estimate(), a.KeyLenDistribution.Min(), a.KeyLenDistribution.Max(), a.KeyLenDistribution.Mean(), a.KeyLenDistribution.StdDev(), a.KeyLenDistribution.ValueAtQuantile(50), a.KeyLenDistribution.ValueAtQuantile(99),
	)

	fmt.Printf("%sValue    |distinct|len-min|len-max|len-mean|len-stdev|len-p50|len-p99|\n", indent)
	if a.I64DistinctValue.Estimate() > 0 {
		fmt.Printf("%sI64    |%8d|     NA|     NA|      NA|       NA|     NA|     NA|\n", indent+"  ", a.I64DistinctValue.Estimate())
	}
	if a.F64DistinctValue.Estimate() > 0 {
		fmt.Printf("%sF64    |%8d|     NA|     NA|      NA|       NA|     NA|     NA|\n", indent+"  ", a.F64DistinctValue.Estimate())
	}
	if a.StringDistinctValue.Estimate() > 0 {
		fmt.Printf("%sString |%8d|%7d|%7d|%8.2f|%9.2f|%7d|%7d|\n", indent+"  ", a.StringDistinctValue.Estimate(),
			a.StringLenDistribution.Min(), a.StringLenDistribution.Max(), a.StringLenDistribution.Mean(), a.StringLenDistribution.StdDev(), a.StringLenDistribution.ValueAtQuantile(50), a.StringLenDistribution.ValueAtQuantile(99),
		)
	}
	if a.BinaryDistinctValue.Estimate() > 0 {
		fmt.Printf("%sBinary |%8d|%7d|%7d|%8.2f|%9.2f|%7d|%7d|\n", indent+"  ", a.BinaryDistinctValue.Estimate())
	}

	fmt.Printf("%sDropped attributes count | count|   min|   max|  mean| stdev|   p50|   p99|\n", indent)
	fmt.Printf("%s                         |%6d|%6d|%6d|%6.1f|%6.1f|%6d|%6d|\n", indent,
		a.DACDistribution.TotalCount(), a.DACDistribution.Min(), a.DACDistribution.Max(), a.DACDistribution.Mean(), a.DACDistribution.StdDev(), a.DACDistribution.ValueAtQuantile(50), a.DACDistribution.ValueAtQuantile(99),
	)
}

func (s *SchemaUrlStats) UpdateWith(schemaUrl string) {
	if schemaUrl == "" {
		s.Missing++
	} else {
		s.NonEmpty++
		RequireNoError(s.SizeDistribution.RecordValue(int64(len(schemaUrl))))
	}
}

func (s *SchemaUrlStats) ShowStats(indent string) {
	fmt.Printf("%sSchemaUrl string length distribution (total-count, missing, min, max, mean, stdev, p50, p99): %d, %d, %d, %d, %f, %f, %d, %d\n", indent,
		s.SizeDistribution.TotalCount(), s.Missing, s.SizeDistribution.Min(), s.SizeDistribution.Max(), s.SizeDistribution.Mean(), s.SizeDistribution.StdDev(), s.SizeDistribution.ValueAtQuantile(50), s.SizeDistribution.ValueAtQuantile(99),
	)
}

func RequireNoError(err error) {
	if err != nil {
		panic(err)
	}
}

func NewStringStats() *StringStats {
	return &StringStats{
		LenDistribution: hdrhistogram.New(0, 1000000, 2),
		DistinctValue:   hyperloglog.New16(),
	}
}

func (s *StringStats) UpdateWith(str string) {
	if len(str) == 0 {
		s.Missing++
	}

	RequireNoError(s.LenDistribution.RecordValue(int64(len(str))))
	s.DistinctValue.Insert([]byte(str))
}

func (s *StringStats) ShowStats(name string, indent string) {
	fmt.Printf("%s%s (count, missing, distinct, len-min, len-max, len-mean, len-stdev, len-p50, len-p99): %d, %d, %d, %d, %d, %f, %f, %d, %d\n", indent, name,
		s.LenDistribution.TotalCount(), s.Missing, s.DistinctValue.Estimate(), s.LenDistribution.Min(), s.LenDistribution.Max(), s.LenDistribution.Mean(), s.LenDistribution.StdDev(), s.LenDistribution.ValueAtQuantile(50), s.LenDistribution.ValueAtQuantile(99),
	)
}

func NewSpanStats() *SpanStats {
	return &SpanStats{
		Distribution:      hdrhistogram.New(0, 1000000, 2),
		Attributes:        NewAttributesStats(),
		TimeIntervalStats: NewTimeIntervalStats(),
		SpanID:            hyperloglog.New16(),
		TraceID:           hyperloglog.New16(),
		ParentSpanID:      hyperloglog.New16(),
		Name:              NewStringStats(),
	}
}

func (s *SpanStats) UpdateWith(spans []*ptrace.Span) {
	RequireNoError(s.Distribution.RecordValue(int64(len(spans))))

	s.TimeIntervalStats.UpdateWithSpans(spans)

	for _, span := range spans {
		s.Attributes.UpdateWith(span.Attributes(), span.DroppedAttributesCount())
		s.Name.UpdateWith(span.Name())
		s.SpanID.Insert([]byte(span.SpanID().String()))
		s.TraceID.Insert([]byte(span.TraceID().String()))
		s.ParentSpanID.Insert([]byte(span.ParentSpanID().String()))

		// Event stats

		// Link stats
	}

	s.TotalCount += int64(len(spans))
}

func (s *SpanStats) ShowStats(indent string) {
	fmt.Printf("%sSpans (min, max, mean, stdev, p50, p99): %d, %d, %f, %f, %d, %d\n", indent,
		s.Distribution.Min(), s.Distribution.Max(), s.Distribution.Mean(), s.Distribution.StdDev(), s.Distribution.ValueAtQuantile(50), s.Distribution.ValueAtQuantile(99),
	)
	indent += "  "
	s.TimeIntervalStats.ShowStats(indent)
	s.Name.ShowStats("Name", indent)
	fmt.Printf("%sID            |distinct|   total|%%distinct|\n", indent)
	fmt.Printf("%s  Span        |%8d|%8d|%8.1f%%|\n", indent, s.SpanID.Estimate(), s.TotalCount, 100.0*float64(s.SpanID.Estimate())/float64(s.TotalCount))
	fmt.Printf("%s  Trace       |%8d|%8d|%8.1f%%|\n", indent, s.TraceID.Estimate(), s.TotalCount, 100.0*float64(s.TraceID.Estimate())/float64(s.TotalCount))
	fmt.Printf("%s  Parent span |%8d|%8d|%8.1f%%|\n", indent, s.ParentSpanID.Estimate(), s.TotalCount, 100.0*float64(s.ParentSpanID.Estimate())/float64(s.TotalCount))
	s.Attributes.ShowStats(indent)
}

func NewTimeIntervalStats() *TimeIntervalStats {
	return &TimeIntervalStats{
		IntervalDistinctValue:   hyperloglog.New16(),
		StartDeltaDistinctValue: hyperloglog.New16(),
		EndDeltaDistinctValue:   hyperloglog.New16(),
	}
}

func (t *TimeIntervalStats) UpdateWithSpans(spans []*ptrace.Span) {
	var prevStartTime time.Time
	var prevEndTime time.Time

	// Process StartDelta
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].StartTimestamp().AsTime().Before(spans[j].StartTimestamp().AsTime())
	})
	for i, span := range spans {
		if i > 0 {
			startDelta := span.StartTimestamp().AsTime().Sub(prevStartTime)

			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(startDelta.Nanoseconds()))
			t.StartDeltaDistinctValue.Insert(b)
		}

		prevStartTime = span.StartTimestamp().AsTime()
	}

	// Process EndDelta
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].EndTimestamp().AsTime().Before(spans[j].EndTimestamp().AsTime())
	})
	for i, span := range spans {
		if i > 0 {
			endDelta := span.EndTimestamp().AsTime().Sub(prevEndTime)

			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(endDelta.Nanoseconds()))
			t.EndDeltaDistinctValue.Insert(b)
		}

		prevEndTime = span.EndTimestamp().AsTime()
	}

	for _, span := range spans {
		interval := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime())
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(interval.Nanoseconds()))
		t.IntervalDistinctValue.Insert(b)

	}

	t.TotalCount += int64(len(spans))
}

func (t *TimeIntervalStats) ShowStats(indent string) {
	fmt.Printf("%sTime interval |distinct|   total|%%distinct|\n", indent)
	indent += "  "
	fmt.Printf("%sEnd-Start   |%8d|%8d|%8.1f%%|\n", indent, t.IntervalDistinctValue.Estimate(), t.TotalCount, 100.0*float64(t.IntervalDistinctValue.Estimate())/float64(t.TotalCount))
	fmt.Printf("%sStart delta |%8d|%8d|%8.1f%%|\n", indent, t.StartDeltaDistinctValue.Estimate(), t.TotalCount, 100.0*float64(t.StartDeltaDistinctValue.Estimate())/float64(t.TotalCount))
	fmt.Printf("%sEnd delta   |%8d|%8d|%8.1f%%|\n", indent, t.EndDeltaDistinctValue.Estimate(), t.TotalCount, 100.0*float64(t.EndDeltaDistinctValue.Estimate())/float64(t.TotalCount))
}
