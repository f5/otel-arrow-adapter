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

package otlpreceiver // import "github.com/f5/otel-arrow-adapter/collector/gen/receiver/otlpreceiver"

import (
	"context"

	"github.com/f5/otel-arrow-adapter/collector/gen/internal/sharedcomponent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	typeStr = "otlp"

	defaultGRPCEndpoint = "0.0.0.0:4317"
	defaultHTTPEndpoint = "0.0.0.0:4318"
)

// NewFactory creates a new OTLP receiver factory.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		receiver.WithTraces(createTraces, component.StabilityLevelStable),
		receiver.WithMetrics(createMetrics, component.StabilityLevelStable),
		receiver.WithLogs(createLog, component.StabilityLevelBeta))
}

// createDefaultConfig creates the default configuration for receiver.
func createDefaultConfig() component.Config {
	return &Config{
		Protocols: Protocols{
			GRPC: &configgrpc.GRPCServerSettings{
				NetAddr: confignet.NetAddr{
					Endpoint:  defaultGRPCEndpoint,
					Transport: "tcp",
				},
				// We almost write 0 bytes, so no need to tune WriteBufferSize.
				ReadBufferSize: 512 * 1024,
			},
			HTTP: &confighttp.HTTPServerSettings{
				Endpoint: defaultHTTPEndpoint,
			},
			Arrow: &ArrowSettings{
				Enabled: false,
			},
		},
	}
}

// createTraces creates a trace receiver based on provided config.
func createTraces(
	_ context.Context,
	set receiver.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (receiver.Traces, error) {
	r, err := receivers.GetOrAdd(cfg, func() (component.Component, error) {
		return newOtlpReceiver(cfg.(*Config), set)
	})
	if err != nil {
		return nil, err
	}

	if err = r.Unwrap().(*otlpReceiver).registerTraceConsumer(nextConsumer); err != nil {
		return nil, err
	}
	return r, nil
}

// createMetrics creates a metrics receiver based on provided config.
func createMetrics(
	_ context.Context,
	set receiver.CreateSettings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	r, err := receivers.GetOrAdd(cfg, func() (component.Component, error) {
		return newOtlpReceiver(cfg.(*Config), set)
	})
	if err != nil {
		return nil, err
	}

	if err = r.Unwrap().(*otlpReceiver).registerMetricsConsumer(consumer); err != nil {
		return nil, err
	}
	return r, nil
}

// createLog creates a log receiver based on provided config.
func createLog(
	_ context.Context,
	set receiver.CreateSettings,
	cfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	r, err := receivers.GetOrAdd(cfg, func() (component.Component, error) {
		return newOtlpReceiver(cfg.(*Config), set)
	})
	if err != nil {
		return nil, err
	}

	if err = r.Unwrap().(*otlpReceiver).registerLogsConsumer(consumer); err != nil {
		return nil, err
	}
	return r, nil
}

// This is the map of already created OTLP receivers for particular configurations.
// We maintain this map because the Factory is asked trace and metric receivers separately
// when it gets CreateTracesReceiver() and CreateMetricsReceiver() but they must not
// create separate objects, they must use one otlpReceiver object per configuration.
// When the receiver is shutdown it should be removed from this map so the same configuration
// can be recreated successfully.
var receivers = sharedcomponent.NewSharedComponents()
