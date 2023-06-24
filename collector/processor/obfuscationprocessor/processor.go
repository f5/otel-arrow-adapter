package obfuscationprocessor

import (
	"context"
	"github.com/cyrildever/feistel"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type obfuscation struct {
	// Logger
	logger *zap.Logger
	// Next trace consumer in line
	next consumer.Traces

	encryptAttributes map[string]struct{}
	encrypt           *feistel.FPECipher
	encryptAll        bool
}

// processTraces implements ProcessMetricsFunc. It processes the incoming data
// and returns the data to be sent to the next component
func (o *obfuscation) processTraces(ctx context.Context, batch ptrace.Traces) (ptrace.Traces, error) {
	for i := 0; i < batch.ResourceSpans().Len(); i++ {
		rs := batch.ResourceSpans().At(i)
		o.processResourceSpan(ctx, rs)
	}
	return batch, nil
}

// processResourceSpan processes the ResourceSpans and all of its spans
func (o *obfuscation) processResourceSpan(ctx context.Context, rs ptrace.ResourceSpans) {
	rsAttrs := rs.Resource().Attributes()

	// Attributes can be part of a resource span
	o.processAttrs(ctx, rsAttrs)

	for j := 0; j < rs.ScopeSpans().Len(); j++ {
		ils := rs.ScopeSpans().At(j)
		for k := 0; k < ils.Spans().Len(); k++ {
			span := ils.Spans().At(k)
			spanAttrs := span.Attributes()

			// Attributes can also be part of span
			o.processAttrs(ctx, spanAttrs)
		}
	}
}

// processAttrs obfuscates the attributes of a resource span or a span
func (o *obfuscation) processAttrs(_ context.Context, attributes pcommon.Map) {
	attributes.Range(func(k string, value pcommon.Value) bool {
		if !o.encryptAll {
			// check if in encryptList
			_, ok := o.encryptAttributes[k]
			if !ok {
				return true
			}
		}

		if value.Type() == pcommon.ValueTypeStr {
			encryptValue := o.encryptString(value.Str())
			value.SetStr(encryptValue)
		}
		return true
	})

}

// Capabilities specifies what this processor does, such as whether it mutates data
func (o *obfuscation) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// Start the obfuscation processor
func (o *obfuscation) Start(_ context.Context, _ component.Host) error {
	return nil
}

// Shutdown the obfuscation processor
func (o *obfuscation) Shutdown(context.Context) error {
	return nil
}

func (o *obfuscation) encryptString(source string) string {
	obfuscated, _ := o.encrypt.Encrypt(source)
	return obfuscated.String(true)
}