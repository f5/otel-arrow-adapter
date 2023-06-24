package obfuscationprocessor

import (
	"context"
	"testing"

	"github.com/cyrildever/feistel"
	"github.com/cyrildever/feistel/common/utils/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var (
	resAttrVal   = "resource-attr-val-1"
	scopeAttrVal = "scope-attr-val-1"
	spanAttrVal  = "span-attr-val-1"
	eventAttrVal = "event-attr-val-1"
	linkAttrVal  = "link-attr-val-1"
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