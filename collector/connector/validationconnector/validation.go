// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package validationconnector

import (
	"context"
	"fmt"
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
		fmt.Println("LOOK", ids, "AND", v.cfg.Follower)
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

func (v *tracesValidation) foreachSpan(td ptrace.Traces, tf func(key spanKey, span ptrace.Span)) {
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
				tf(key, span)
			}
		}
	}
}

func (v *tracesValidation) expecting(td ptrace.Traces) {
	v.lock.Lock()
	defer v.lock.Unlock()

	v.foreachSpan(td, func(key spanKey, span ptrace.Span) {
		if _, ok := v.store[key]; ok {
			v.logger.Error("duplicate test input span", zap.Reflect("found", key))
			return
		}
		v.store[key] = span
	})
}

func (v *tracesValidation) received(td ptrace.Traces) error {
	v.lock.Lock()
	defer v.lock.Unlock()

	v.foreachSpan(td, func(key spanKey, span ptrace.Span) {
		_, ok := v.store[key]
		if !ok {
			v.logger.Info("span not expected")
			fmt.Println(key.ResAttrs.ToSlice())
			return
		}
		delete(v.store, key)
	})

	return nil
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
	return consumer.Capabilities{MutatesData: false}
}

func (v *validation) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (v *validation) Shutdown(ctx context.Context) error {
	return nil
}

func (v *tracesValidation) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if v.cfg.hasFollower() {
		// Input outputs as to its follower first.
		ctx = context.WithValue(ctx, inputToOutputContext{}, struct{}{})
	} else if ctx.Value(inputToOutputContext{}) != nil {
		// Output expected test input.
		v.expecting(td)
	} else if err := v.received(td); err != nil {
		// Output validating actual input failed.
		return err
	}

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
