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

package otlpexporter // import "github.com/f5/otel-arrow-adapter/collector/exporter/otlpexporter"

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	arrowPkg "github.com/apache/arrow/go/v10/arrow"
	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	arrowRecord "github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"
	"go.uber.org/multierr"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/f5/otel-arrow-adapter/collector/exporter/otlpexporter/internal/arrow"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

type baseExporter struct {
	// Input configuration.
	config *Config

	// gRPC clients and connection.
	traceExporter  ptraceotlp.GRPCClient
	metricExporter pmetricotlp.GRPCClient
	logExporter    plogotlp.GRPCClient
	clientConn     *grpc.ClientConn
	metadata       metadata.MD
	callOptions    []grpc.CallOption

	settings component.ExporterCreateSettings

	// Default user-agent header.
	userAgent string

	// OTLP+Arrow optional state
	arrow *arrow.Exporter
}

// Crete new exporter and start it. The exporter will begin connecting but
// this function may return before the connection is established.
func newExporter(cfg component.Config, set exporter.CreateSettings) (*baseExporter, error) {
	oCfg := cfg.(*Config)

	if oCfg.Endpoint == "" {
		return nil, errors.New("OTLP exporter config requires an Endpoint")
	}

	userAgent := fmt.Sprintf("%s/%s (%s/%s)",
		set.BuildInfo.Description, set.BuildInfo.Version, runtime.GOOS, runtime.GOARCH)

	if oCfg.Arrow != nil && oCfg.Arrow.Enabled {
		userAgent += fmt.Sprintf(" ApacheArrow/%s (NumStreams/%d)", arrowPkg.PkgVersion, oCfg.Arrow.NumStreams)
	}

	return &baseExporter{config: oCfg, settings: set, userAgent: userAgent}, nil
}

// start actually creates the gRPC connection. The client construction is deferred till this point as this
// is the only place we get hold of Extensions which are required to construct auth round tripper.
func (e *baseExporter) start(ctx context.Context, host component.Host) (err error) {
	if e.clientConn, err = e.config.GRPCClientSettings.ToClientConn(ctx, host, e.settings.TelemetrySettings, grpc.WithUserAgent(e.userAgent)); err != nil {
		return err
	}
	e.traceExporter = ptraceotlp.NewGRPCClient(e.clientConn)
	e.metricExporter = pmetricotlp.NewGRPCClient(e.clientConn)
	e.logExporter = plogotlp.NewGRPCClient(e.clientConn)
	e.metadata = metadata.New(e.config.GRPCClientSettings.Headers)
	e.callOptions = []grpc.CallOption{
		grpc.WaitForReady(e.config.GRPCClientSettings.WaitForReady),
	}

	if e.config.Arrow != nil && e.config.Arrow.Enabled {
		ctx := e.enhanceContext(context.Background())

		e.arrow = arrow.NewExporter(*e.config.Arrow, func() arrowRecord.ProducerAPI {
			return arrowRecord.NewProducer()
		}, e.settings.TelemetrySettings, arrowpb.NewArrowStreamServiceClient(e.clientConn), e.callOptions)

		if err := e.arrow.Start(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (e *baseExporter) shutdown(ctx context.Context) error {
	var err error
	if e.arrow != nil {
		err = multierr.Append(err, e.arrow.Shutdown(ctx))
	}
	if e.clientConn != nil {
		err = multierr.Append(err, e.clientConn.Close())
	}
	return err
}

// arrowSendAndWait gets an available stream and tries to send using
// Arrow if it is configured.  A (false, nil) result indicates for the
// caller to fall back to ordinary OTLP.
func (e *baseExporter) arrowSendAndWait(ctx context.Context, data interface{}) (sent bool, _ error) {
	if e.arrow == nil {
		return false, nil
	}
	return e.arrow.SendAndWait(ctx, data)
}

func (e *baseExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	if sent, err := e.arrowSendAndWait(ctx, td); err != nil {
		return err
	} else if sent {
		return nil
	}
	req := ptraceotlp.NewExportRequestFromTraces(td)
	_, err := e.traceExporter.Export(e.enhanceContext(ctx), req, e.callOptions...)
	return processGRPCError(err)
}

func (e *baseExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	if sent, err := e.arrowSendAndWait(ctx, md); err != nil {
		return err
	} else if sent {
		return nil
	}
	req := pmetricotlp.NewExportRequestFromMetrics(md)
	_, err := e.metricExporter.Export(e.enhanceContext(ctx), req, e.callOptions...)
	return processGRPCError(err)
}

func (e *baseExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	if sent, err := e.arrowSendAndWait(ctx, ld); err != nil {
		return err
	} else if sent {
		return nil
	}
	req := plogotlp.NewExportRequestFromLogs(ld)
	_, err := e.logExporter.Export(e.enhanceContext(ctx), req, e.callOptions...)
	return processGRPCError(err)
}

func (e *baseExporter) enhanceContext(ctx context.Context) context.Context {
	if e.metadata.Len() > 0 {
		return metadata.NewOutgoingContext(ctx, e.metadata)
	}
	return ctx
}

func processGRPCError(err error) error {
	if err == nil {
		// Request is successful, we are done.
		return nil
	}

	// We have an error, check gRPC status code.

	st := status.Convert(err)
	if st.Code() == codes.OK {
		// Not really an error, still success.
		return nil
	}

	// Now, this is this a real error.

	retryInfo := getRetryInfo(st)

	if !shouldRetry(st.Code(), retryInfo) {
		// It is not a retryable error, we should not retry.
		return consumererror.NewPermanent(err)
	}

	// Check if server returned throttling information.
	throttleDuration := getThrottleDuration(retryInfo)
	if throttleDuration != 0 {
		// We are throttled. Wait before retrying as requested by the server.
		return exporterhelper.NewThrottleRetry(err, throttleDuration)
	}

	// Need to retry.

	return err
}

func shouldRetry(code codes.Code, retryInfo *errdetails.RetryInfo) bool {
	switch code {
	case codes.Canceled,
		codes.DeadlineExceeded,
		codes.Aborted,
		codes.OutOfRange,
		codes.Unavailable,
		codes.DataLoss:
		// These are retryable errors.
		return true
	case codes.ResourceExhausted:
		// Retry only if RetryInfo was supplied by the server.
		// This indicates that the server can still recover from resource exhaustion.
		return retryInfo != nil
	}
	// Don't retry on any other code.
	return false
}

func getRetryInfo(status *status.Status) *errdetails.RetryInfo {
	for _, detail := range status.Details() {
		if t, ok := detail.(*errdetails.RetryInfo); ok {
			return t
		}
	}
	return nil
}

func getThrottleDuration(t *errdetails.RetryInfo) time.Duration {
	if t == nil || t.RetryDelay == nil {
		return 0
	}
	if t.RetryDelay.Seconds > 0 || t.RetryDelay.Nanos > 0 {
		return time.Duration(t.RetryDelay.Seconds)*time.Second + time.Duration(t.RetryDelay.Nanos)*time.Nanosecond
	}
	return 0
}
