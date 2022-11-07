/*
 * // Copyright The OpenTelemetry Authors
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //       http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 *
 */

package arrow_record

import (
	"bytes"

	"github.com/apache/arrow/go/v10/arrow/ipc"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	colarspb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	logs_otlp "github.com/f5/otel-arrow-adapter/pkg/otel/logs/otlp"
	traces_otlp "github.com/f5/otel-arrow-adapter/pkg/otel/traces/otlp"
)

// ConsumerAPI is the interface of a Consumer consdiering all signals.
// This is useful for mock testing.
type ConsumerAPI interface {
	LogsFrom(*colarspb.BatchArrowRecords) ([]plog.Logs, error)
	TracesFrom(*colarspb.BatchArrowRecords) ([]ptrace.Traces, error)
}

var _ ConsumerAPI = &Consumer{}

// Consumer is a BatchArrowRecords consumer.
type Consumer struct {
	streamConsumers map[string]*streamConsumer
}

type streamConsumer struct {
	bufReader *bytes.Reader
	ipcReader *ipc.Reader
}

// NewConsumer creates a new BatchArrowRecords consumer.
func NewConsumer() *Consumer {
	return &Consumer{
		streamConsumers: make(map[string]*streamConsumer),
	}
}

// LogsFrom produces an array of [plog.Logs] from a BatchArrowRecords message.
func (c *Consumer) LogsFrom(bar *colarspb.BatchArrowRecords) ([]plog.Logs, error) {
	records, err := c.Consume(bar)
	if err != nil {
		return nil, err
	}

	record2Logs := func(record *RecordMessage) (plog.Logs, error) {
		defer record.record.Release()
		return logs_otlp.LogsFrom(record.record)
	}

	var result []plog.Logs
	for i := 1; i < len(records); i++ {
		record := records[i]
		logs, err := record2Logs(record)
		if err != nil {
			return nil, err
		}
		result = append(result, logs)
	}
	return result, nil
}

// TracesFrom produces an array of [ptrace.Traces] from a BatchArrowRecords message.
func (c *Consumer) TracesFrom(bar *colarspb.BatchArrowRecords) ([]ptrace.Traces, error) {
	records, err := c.Consume(bar)
	if err != nil {
		return nil, err
	}

	record2Traces := func(record *RecordMessage) (ptrace.Traces, error) {
		defer record.record.Release()
		return traces_otlp.TracesFrom(record.record)
	}

	var result []ptrace.Traces
	for i := 1; i < len(records); i++ {
		record := records[i]
		traces, err := record2Traces(record)
		if err != nil {
			return nil, err
		}
		result = append(result, traces)
	}
	return result, nil
}

// Consume takes a BatchArrowRecords protobuf message and returns an array of RecordMessage.
func (c *Consumer) Consume(bar *colarspb.BatchArrowRecords) ([]*RecordMessage, error) {

	var ibes []*RecordMessage

	// Transform each individual OtlpArrowPayload into RecordMessage
	for _, payload := range bar.OtlpArrowPayloads {
		// Retrieves (or creates) the stream consumer for the sub-stream id defined in the BatchArrowRecords message.
		sc := c.streamConsumers[payload.SubStreamId]
		if sc == nil {
			bufReader := bytes.NewReader([]byte{})
			sc = &streamConsumer{
				bufReader: bufReader,
			}
			c.streamConsumers[payload.SubStreamId] = sc
		}

		sc.bufReader.Reset(payload.Record)
		if sc.ipcReader == nil {
			ipcReader, err := ipc.NewReader(sc.bufReader)
			if err != nil {
				return nil, err
			}
			sc.ipcReader = ipcReader
		}

		if sc.ipcReader.Next() {
			rec := sc.ipcReader.Record()
			ibes = append(ibes, &RecordMessage{
				batchId:      bar.BatchId,
				payloadType:  payload.GetType(),
				record:       rec,
				deliveryType: bar.DeliveryType,
			})
		}
	}

	return ibes, nil
}
