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

package batch_event

import (
	"bytes"

	"github.com/apache/arrow/go/v9/arrow/ipc"

	coleventspb "github.com/lquerel/otel-arrow-adapter/api/collector/arrow/v1"
)

type Consumer struct {
	streamConsumers map[string]*streamConsumer
}

type streamConsumer struct {
	bufReader *bytes.Reader
	ipcReader *ipc.Reader
}

// NewConsumer creates a new ArrowBatch consumer.
func NewConsumer() *Consumer {
	return &Consumer{
		streamConsumers: make(map[string]*streamConsumer),
	}
}

// Consume takes a ArrowBatch protobuf message and returns an array of RecordMessage.
func (c *Consumer) Consume(event *coleventspb.ArrowBatch) ([]*RecordMessage, error) {
	// Retrieves (or creates) the stream consumer for the sub-stream id defined in the ArrowBatch message.
	sc := c.streamConsumers[event.SubStreamId]
	if sc == nil {
		bufReader := bytes.NewReader([]byte{})
		sc = &streamConsumer{
			bufReader: bufReader,
		}
		c.streamConsumers[event.SubStreamId] = sc
	}

	var ibes []*RecordMessage

	// Transform each individual Payload into RecordMessage
	for _, payload := range event.Payloads {
		sc.bufReader.Reset(payload.Schema) // ToDo change the protobuf definition to contain a single ipc_message
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
				batchId:      event.BatchId,
				recordType:   payload.GetType(),
				record:       rec,
				deliveryType: event.DeliveryType,
			})
		}
	}

	return ibes, nil
}
