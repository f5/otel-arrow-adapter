// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package validationconnector

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

const (
	typeStr = "validation"
)

type inputToOutputContext struct{}

type Config struct {
	Follower component.ID `mapstructure:"follower"`
}

type validation struct {
	lock   sync.Mutex
	cfg    *Config
	logger *zap.Logger
}

// spanKey includes all structural fields outside the ptrace.Span.
type spanKey struct {
	ResAttrs       attribute.Set
	ResAttrsDAC    int
	ResSchemaURL   string
	ScopeAttrs     attribute.Set
	ScopeAttrsDAC  int
	ScopeName      string
	ScopeVersion   string
	ScopeSchemaURL string
	SpanName       string
	SpanAttrs      attribute.Set
	SpanID         pcommon.SpanID
}

func attrsToString(m attribute.Set) string {
	var sb strings.Builder
	for _, attr := range m.ToSlice() {
		sb.WriteString(fmt.Sprint(attr.Key, "=", attr.Value, "\n"))
	}
	return sb.String()
}

func (s spanKey) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprint("span_name=", s.SpanName, "\n"))
	sb.WriteString(fmt.Sprint("span_id=", s.SpanID, "\n"))
	sb.WriteString(fmt.Sprint("res_attrs=", attrsToString(s.ResAttrs), "\n"))
	return sb.String()
}

func spanToString(s ptrace.Span) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprint("span_name=", s.Name(), "\n"))
	sb.WriteString(fmt.Sprint("span_id=", s.SpanID(), "\n"))
	return sb.String()
}

type tracesValidation struct {
	validation
	store map[spanKey]ptrace.Span
	next  consumer.Traces
}

type metricsValidation struct {
	validation
	next consumer.Metrics
}

type logsValidation struct {
	validation
	next consumer.Logs
}

var (
	errUnexpectedConsumer = fmt.Errorf("expected a connector router as consumer")
	errMissingFollower    = fmt.Errorf("validation input should have validation output as follower")
)

func NewFactory() connector.Factory {
	return connector.NewFactory(
		typeStr,
		createDefaultConfig,
		connector.WithTracesToTraces(createTracesToTraces, component.StabilityLevelBeta),
		connector.WithMetricsToMetrics(createMetricsToMetrics, component.StabilityLevelBeta),
		connector.WithLogsToLogs(createLogsToLogs, component.StabilityLevelBeta),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func (c *Config) hasFollower() bool {
	return c.Follower.Type() != ""
}

func (v *validation) reorder(ids []component.ID) ([]component.ID, error) {
	var ordered []component.ID

	found := false
	if v.cfg.hasFollower() {
		ordered = append(ordered, v.cfg.Follower)
	}
	for _, pid := range ids {
		if v.cfg.hasFollower() && v.cfg.Follower == pid {
			found = true
			continue
		}
		ordered = append(ordered, pid)
	}
	if v.cfg.hasFollower() && !found {
		return nil, errMissingFollower
	}
	return ordered, nil
}

func (v *validation) toSet(in pcommon.Map) attribute.Set {
	var attrs []attribute.KeyValue
	in.Range(func(key string, value pcommon.Value) bool {
		switch value.Type() {
		case pcommon.ValueTypeStr:
			attrs = append(attrs, attribute.String(key, value.Str()))
		case pcommon.ValueTypeInt:
			attrs = append(attrs, attribute.Int64(key, value.Int()))
		case pcommon.ValueTypeDouble:
			attrs = append(attrs, attribute.Float64(key, value.Double()))
		case pcommon.ValueTypeBool:
			attrs = append(attrs, attribute.Bool(key, value.Bool()))
		default:
			v.logger.Error("value not supported", zap.String("type", value.Type().String()))
		}
		return true
	})
	return attribute.NewSet(attrs...)
}

func (v *tracesValidation) foreachSpan(td ptrace.Traces, tf func(key spanKey, span ptrace.Span) error) error {
	for ri := 0; ri < td.ResourceSpans().Len(); ri++ {
		rs := td.ResourceSpans().At(ri)
		rattrs := v.toSet(rs.Resource().Attributes())

		for si := 0; si < rs.ScopeSpans().Len(); si++ {
			ss := rs.ScopeSpans().At(si)
			sattrs := v.toSet(ss.Scope().Attributes())

			for i := 0; i < ss.Spans().Len(); i++ {
				span := ss.Spans().At(i)
				key := spanKey{
					ResAttrs:       rattrs,
					ResAttrsDAC:    int(rs.Resource().DroppedAttributesCount()),
					ResSchemaURL:   rs.SchemaUrl(),
					ScopeAttrs:     sattrs,
					ScopeAttrsDAC:  int(ss.Scope().DroppedAttributesCount()),
					ScopeName:      ss.Scope().Name(),
					ScopeVersion:   ss.Scope().Version(),
					ScopeSchemaURL: ss.SchemaUrl(),
					SpanName:       span.Name(),
					SpanAttrs:      v.toSet(span.Attributes()),
					SpanID:         span.SpanID(),
				}
				if err := tf(key, span); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (v *tracesValidation) expecting(td ptrace.Traces) {
	v.lock.Lock()
	defer v.lock.Unlock()

	err := v.foreachSpan(td, func(key spanKey, span ptrace.Span) error {
		if have, ok := v.store[key]; ok {
			v.logger.Info("duplicate test input span", zap.String("key", key.String()), zap.String("s2", spanToString(span)), zap.String("s1", spanToString(have)))
			return fmt.Errorf("Stop the test!")
		}
		fmt.Println("expecting to receive span key", key, "data", spanToString(span))
		v.store[key] = span
		return nil
	})

	if err != nil {
		panic(err)
	}
}

func (v *tracesValidation) received(td ptrace.Traces) error {
	v.lock.Lock()
	defer v.lock.Unlock()

	return v.foreachSpan(td, func(key spanKey, span ptrace.Span) error {
		_, ok := v.store[key]
		if !ok {
			v.logger.Info("test input span not found", zap.String("key", key.String()), zap.String("expect", spanToString(span)))

			for havekey, have := range v.store {
				if have.Name() == span.Name() && have.SpanID() == span.SpanID() {
					fmt.Println("possible match span", spanToString(have), "having received", spanToString(span))
					fmt.Println("received key", key)
					fmt.Println("expected key", havekey)
				}
			}
			return fmt.Errorf("stop the test!")
		}
		// @@@ TODO Require have == expect
		delete(v.store, key)
		return nil
	})
}

func createTracesToTraces(
	ctx context.Context,
	set connector.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (connector.Traces, error) {
	v := &tracesValidation{}
	v.cfg = cfg.(*Config)
	v.logger = set.Logger
	v.store = map[spanKey]ptrace.Span{}

	tr, ok := nextConsumer.(connector.TracesRouter)
	if !ok {
		return nil, errUnexpectedConsumer
	}
	ordered, err := v.reorder(tr.PipelineIDs())
	if err != nil {
		return nil, err
	}
	next, err := tr.Consumer(ordered...)
	if err != nil {
		return nil, err
	}
	v.next = next
	return v, nil
}

func createMetricsToMetrics(
	ctx context.Context,
	set connector.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Metrics, error) {
	v := &metricsValidation{}
	v.cfg = cfg.(*Config)
	v.logger = set.Logger

	tr, ok := nextConsumer.(connector.MetricsRouter)
	if !ok {
		return nil, errUnexpectedConsumer
	}
	ordered, err := v.reorder(tr.PipelineIDs())
	if err != nil {
		return nil, err
	}
	next, err := tr.Consumer(ordered...)
	if err != nil {
		return nil, err
	}
	v.next = next
	return v, nil
}

func createLogsToLogs(
	ctx context.Context,
	set connector.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (connector.Logs, error) {
	v := &logsValidation{}
	v.cfg = cfg.(*Config)
	v.logger = set.Logger

	tr, ok := nextConsumer.(connector.LogsRouter)
	if !ok {
		return nil, errUnexpectedConsumer
	}
	ordered, err := v.reorder(tr.PipelineIDs())
	if err != nil {
		return nil, err
	}
	next, err := tr.Consumer(ordered...)
	if err != nil {
		return nil, err
	}
	v.next = next

	return v, nil
}

func (v *validation) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (v *validation) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (v *validation) Shutdown(ctx context.Context) error {
	return nil
}

func (v *tracesValidation) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if v.cfg.hasFollower() {
		fmt.Println("1st stage received spans", td.SpanCount())
		// Input outputs as to its follower first.
		ctx = context.WithValue(ctx, inputToOutputContext{}, struct{}{})
		return v.next.ConsumeTraces(ctx, td)
	}

	if ctx.Value(inputToOutputContext{}) != nil {
		fmt.Println("2nd stage expecting spans", td.SpanCount())
		// Output expected test input.  Do not consume.
		v.expecting(td)
		return nil
	}

	if err := v.received(td); err != nil {
		fmt.Println("2nd stage FAILED spans", td.SpanCount(), err)
		// Output validating actual input failed.
		return err
	}
	defer fmt.Println("2nd stage validated spans", td.SpanCount())
	return v.next.ConsumeTraces(ctx, td)
}

func (v *metricsValidation) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// @@@
	return v.next.ConsumeMetrics(ctx, md)
}

func (v *logsValidation) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	// @@@
	return v.next.ConsumeLogs(ctx, ld)
}
