package obfuscationprocessor

import (
	"context"
	"testing"

	"github.com/cyrildever/feistel"
	"github.com/cyrildever/feistel/common/utils/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var (
	resAttrVal   = "resource-attr-val-1"
	scopeAttrVal = "scope-attr-val-1"

	// span specific attrs
	spanAttrVal  = "span-attr-val-1"
	eventAttrVal = "event-attr-val-1"
	linkAttrVal  = "link-attr-val-1"

	// metric specific attrs
	gaugeAttrVal   = "gauge-attr-val-1"
	sumAttrVal     = "sum-attr-val-1"
	histAttrVal    = "hist-attr-val-1"
	eHistAttrVal   = "exp-hist-attr-val-1"
	summaryAttrVal = "summary-attr-val-1"

	// log specific attrs
	logAttrVal = "log-attr-val-1"
)

func setupSpanWithAttrs() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()

	rs.Resource().Attributes().PutStr("resource-attr", resAttrVal)

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().Attributes().PutStr("scope-attr", scopeAttrVal)

	span := ss.Spans().AppendEmpty()
	span.SetName("operationA")
	span.Attributes().PutStr("span-attr", spanAttrVal)

	link0 := span.Links().AppendEmpty()
	link0.Attributes().PutStr("span-link-attr", linkAttrVal)
	ev0 := span.Events().AppendEmpty()
	ev0.Attributes().PutStr("span-event-attr", eventAttrVal)

	return td
}

func validateTraceAttrs(t *testing.T, expected map[string]string, traces ptrace.Traces) {
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		// validate resource attributes
		rs := traces.ResourceSpans().At(i)
		val, ok := rs.Resource().Attributes().Get("resource-attr")
		assert.True(t, ok)
		assert.Equal(t, expected["resource-attr"], val.AsString())

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			// validate scope attributes
			ss := rs.ScopeSpans().At(j)
			scopeVal, ok := ss.Scope().Attributes().Get("scope-attr")
			assert.True(t, ok)
			assert.Equal(t, expected["scope-attr"], scopeVal.AsString())

			for k := 0; k < ss.Spans().Len(); k++ {
				// validate span attributes
				span := ss.Spans().At(k)
				val, ok := span.Attributes().Get("span-attr")
				assert.True(t, ok)
				assert.Equal(t, expected["span-attr"], val.AsString())

				for h := 0; h < span.Events().Len(); h++ {
					// validate event attributes
					event := span.Events().At(h)
					val, ok := event.Attributes().Get("span-event-attr")
					assert.True(t, ok)
					assert.Equal(t, expected["span-event-attr"], val.AsString())
				}

				for h := 0; h < span.Links().Len(); h++ {
					// validate link attributes
					link := span.Links().At(h)
					val, ok := link.Attributes().Get("span-link-attr")
					assert.True(t, ok)
					assert.Equal(t, expected["span-link-attr"], val.AsString())
				}
			}
		}
	}
}

func TestProcessTraces(t *testing.T) {
	traces := setupSpanWithAttrs()

	processor := &obfuscation{
		encrypt:           feistel.NewFPECipher(hash.SHA_256, "some-32-byte-long-key-to-be-safe", 128),
		encryptAll:        true,
	}

	obfsResAttr, _ := processor.encrypt.Encrypt(resAttrVal)
	obfsScopeAttr, _ := processor.encrypt.Encrypt(scopeAttrVal)
	obfsSpanAttr, _ := processor.encrypt.Encrypt(spanAttrVal)
	obfsEventAttr, _ := processor.encrypt.Encrypt(eventAttrVal)
	obfsLinkAttr, _ := processor.encrypt.Encrypt(linkAttrVal)
	expected := map[string]string{
		"resource-attr":   obfsResAttr.String(true),
		"scope-attr":      obfsScopeAttr.String(true),
		"span-attr":       obfsSpanAttr.String(true),
		"span-link-attr":  obfsLinkAttr.String(true),
		"span-event-attr": obfsEventAttr.String(true),
	}

	processedTraces, err := processor.processTraces(context.Background(), traces)
	require.NoError(t, err)
	validateTraceAttrs(t, expected, processedTraces)
}


func setupMetricsWithAttrs() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()

	rm.Resource().Attributes().PutStr("resource-attr", resAttrVal)

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().Attributes().PutStr("scope-attr", scopeAttrVal)

	metric := sm.Metrics().AppendEmpty()
	gauge := metric.SetEmptyGauge()
	gdp := gauge.DataPoints().AppendEmpty()
	gdp.Attributes().PutStr("gauge-attr", gaugeAttrVal)

	metric = sm.Metrics().AppendEmpty()
	sum := metric.SetEmptySum()
	sdp := sum.DataPoints().AppendEmpty()
	sdp.Attributes().PutStr("sum-attr", sumAttrVal)

	metric = sm.Metrics().AppendEmpty()
	hist := metric.SetEmptyHistogram()
	hdp := hist.DataPoints().AppendEmpty()
	hdp.Attributes().PutStr("histogram-attr", histAttrVal)

	metric = sm.Metrics().AppendEmpty()
	eHist := metric.SetEmptyExponentialHistogram()
	ehdp := eHist.DataPoints().AppendEmpty()
	ehdp.Attributes().PutStr("exp-histogram-attr", eHistAttrVal)

	metric = sm.Metrics().AppendEmpty()
	summary := metric.SetEmptySummary()
	smdp := summary.DataPoints().AppendEmpty()
	smdp.Attributes().PutStr("summary-attr", summaryAttrVal)

	return md
}

func validateMetricsAttrs(t *testing.T, expected map[string]string, metrics pmetric.Metrics) {
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		// validate resource attributes
		rm := metrics.ResourceMetrics().At(i)
		val, ok := rm.Resource().Attributes().Get("resource-attr")
		assert.True(t, ok)
		assert.Equal(t, expected["resource-attr"], val.AsString())

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			// validate scope attributes
			sm := rm.ScopeMetrics().At(j)
			scopeVal, ok := sm.Scope().Attributes().Get("scope-attr")
			assert.True(t, ok)
			assert.Equal(t, expected["scope-attr"], scopeVal.AsString())

			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)

				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					gdp := metric.Gauge().DataPoints()
					for i := 0; i < gdp.Len(); i++ {
						dp := gdp.At(i)
						val, ok := dp.Attributes().Get("gauge-attr")
						assert.True(t, ok)
						assert.Equal(t, expected["gauge-attr"], val.AsString())
					}

				case pmetric.MetricTypeSum:
					sdp := metric.Sum().DataPoints()
					for i := 0; i < sdp.Len(); i++ {
						dp := sdp.At(i)
						val, ok := dp.Attributes().Get("sum-attr")
						assert.True(t, ok)
						assert.Equal(t, expected["sum-attr"], val.AsString())
					}

				case pmetric.MetricTypeHistogram:
					hdp := metric.Histogram().DataPoints()
					for i := 0; i < hdp.Len(); i++ {
						dp := hdp.At(i)
						val, ok := dp.Attributes().Get("histogram-attr")
						assert.True(t, ok)
						assert.Equal(t, expected["histogram-attr"], val.AsString())
					}

				case pmetric.MetricTypeExponentialHistogram:
					ehdp := metric.ExponentialHistogram().DataPoints()
					for i := 0; i < ehdp.Len(); i++ {
						dp := ehdp.At(i)
						val, ok := dp.Attributes().Get("exp-histogram-attr")
						assert.True(t, ok)
						assert.Equal(t, expected["exp-histogram-attr"], val.AsString())
					}

				case pmetric.MetricTypeSummary:
					smdp := metric.Summary().DataPoints()
					for i := 0; i < smdp.Len(); i++ {
						dp := smdp.At(i)
						val, ok := dp.Attributes().Get("summary-attr")
						assert.True(t, ok)
						assert.Equal(t, expected["summary-attr"], val.AsString())
					}
				}
			}
		}
	}
}

func TestProcessMetrics(t *testing.T) {
	metrics := setupMetricsWithAttrs()

	processor := &obfuscation{
		encrypt:           feistel.NewFPECipher(hash.SHA_256, "some-32-byte-long-key-to-be-safe", 128),
		encryptAll:        true,
	}

	obfsResAttr, _ := processor.encrypt.Encrypt(resAttrVal)
	obfsScopeAttr, _ := processor.encrypt.Encrypt(scopeAttrVal)
	obfsGaugeAttr, _ := processor.encrypt.Encrypt(gaugeAttrVal)
	obfsSumAttr, _ := processor.encrypt.Encrypt(sumAttrVal)
	obfsHistAttr, _ := processor.encrypt.Encrypt(histAttrVal)
	obfsEHistAttr, _ := processor.encrypt.Encrypt(eHistAttrVal)
	obfsSummaryAttr, _ := processor.encrypt.Encrypt(summaryAttrVal)

	expected := map[string]string{
		"resource-attr":      obfsResAttr.String(true),
		"scope-attr":         obfsScopeAttr.String(true),
		"gauge-attr":         obfsGaugeAttr.String(true),
		"sum-attr":           obfsSumAttr.String(true),
		"histogram-attr":     obfsHistAttr.String(true),
		"exp-histogram-attr": obfsEHistAttr.String(true),
		"summary-attr":       obfsSummaryAttr.String(true),
	}

	processedMetrics, err := processor.processMetrics(context.Background(), metrics)
	require.NoError(t, err)
	validateMetricsAttrs(t, expected, processedMetrics)
}

func setupLogsWithAttrs() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()

	rl.Resource().Attributes().PutStr("resource-attr", resAttrVal)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().Attributes().PutStr("scope-attr", scopeAttrVal)

	log := sl.LogRecords().AppendEmpty()
	log.Attributes().PutStr("log-attr", logAttrVal)

	return ld
}

func validateLogsAttrs(t *testing.T, expected map[string]string, logs plog.Logs) {
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		// validate resource attributes
		rl := logs.ResourceLogs().At(i)
		val, ok := rl.Resource().Attributes().Get("resource-attr")
		assert.True(t, ok)
		assert.Equal(t, expected["resource-attr"], val.AsString())

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			// validate scope attributes
			sl := rl.ScopeLogs().At(j)
			scopeVal, ok := sl.Scope().Attributes().Get("scope-attr")
			assert.True(t, ok)
			assert.Equal(t, expected["scope-attr"], scopeVal.AsString())

			for k := 0; k < sl.LogRecords().Len(); k++ {
				// validate span attributes
				log := sl.LogRecords().At(k)
				val, ok := log.Attributes().Get("log-attr")
				assert.True(t, ok)
				assert.Equal(t, expected["log-attr"], val.AsString())
			}
		}
	}
}

func TestProcessLogs(t *testing.T) {
	logs := setupLogsWithAttrs()

	processor := &obfuscation{
		encrypt:           feistel.NewFPECipher(hash.SHA_256, "some-32-byte-long-key-to-be-safe", 128),
		encryptAll:        true,
	}

	obfsResAttr, _ := processor.encrypt.Encrypt(resAttrVal)
	obfsScopeAttr, _ := processor.encrypt.Encrypt(scopeAttrVal)
	obfsLogAttr, _ := processor.encrypt.Encrypt(logAttrVal)
	expected := map[string]string{
		"resource-attr": obfsResAttr.String(true),
		"scope-attr":    obfsScopeAttr.String(true),
		"log-attr":      obfsLogAttr.String(true),
	}

	processedLogs, err := processor.processLogs(context.Background(), logs)
	require.NoError(t, err)
	validateLogsAttrs(t, expected, processedLogs)
}