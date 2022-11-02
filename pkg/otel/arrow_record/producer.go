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

package arrow_record

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v10/arrow/ipc"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/collector/pdata/ptrace"

	colarspb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	"github.com/f5/otel-arrow-adapter/pkg/air/config"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	traces_arrow "github.com/f5/otel-arrow-adapter/pkg/otel/traces/arrow"
)

// Producer is a BatchArrowRecords producer.
type Producer struct {
	pool                    *memory.GoAllocator
	streamProducers         map[string]*streamProducer
	otlpArrowTracesProducer *arrow.OtlpArrowProducer[ptrace.ScopeSpans]
	batchId                 int64
}

type streamProducer struct {
	output      bytes.Buffer
	ipcWriter   *ipc.Writer
	subStreamId string
}

// NewProducer creates a new BatchArrowRecords producer.
func NewProducer() *Producer {
	return &Producer{
		pool:                    memory.NewGoAllocator(),
		streamProducers:         make(map[string]*streamProducer),
		otlpArrowTracesProducer: arrow.NewOtlpArrowProducer[ptrace.ScopeSpans](),
		batchId:                 0,
	}
}

// NewProducerWithConfig create a new BatchArrowRecords producer with the given configuration.
func NewProducerWithConfig(cfg *config.Config) *Producer {
	return &Producer{
		streamProducers:         make(map[string]*streamProducer),
		otlpArrowTracesProducer: arrow.NewOtlpArrowProducerWithConfig[ptrace.ScopeSpans](cfg),
		batchId:                 0,
	}
}

// BatchArrowRecordsFrom produces a BatchArrowRecords message from a ptrace.Traces messages.
func (p *Producer) BatchArrowRecordsFrom(ts ptrace.Traces) (*colarspb.BatchArrowRecords, error) {
	tb := traces_arrow.NewTracesBuilder(p.pool)
	if err := tb.Append(ts); err != nil {
		return nil, err
	}
	record, err := tb.Build()
	if err != nil {
		return nil, err
	}

	rms := []*RecordMessage{NewTraceMessage(record, colarspb.DeliveryType_BEST_EFFORT)}

	bar, err := p.Produce(rms, colarspb.DeliveryType_BEST_EFFORT)
	if err != nil {
		return nil, err
	}
	return bar, nil
}

// Produce takes a slice of RecordMessage and returns the corresponding BatchArrowRecords protobuf message.
func (p *Producer) Produce(rms []*RecordMessage, deliveryType colarspb.DeliveryType) (*colarspb.BatchArrowRecords, error) {
	oapl := make([]*colarspb.OtlpArrowPayload, len(rms))

	for i, rm := range rms {
		// Retrieves (or creates) the stream Producer for the sub-stream id defined in the RecordMessage.
		sp := p.streamProducers[rm.subStreamId]
		if sp == nil {
			var buf bytes.Buffer
			sp = &streamProducer{
				output:      buf,
				subStreamId: fmt.Sprintf("%d", len(p.streamProducers)),
			}
			p.streamProducers[rm.subStreamId] = sp
		}

		if sp.ipcWriter == nil {
			sp.ipcWriter = ipc.NewWriter(&sp.output, ipc.WithSchema(rm.record.Schema()))
		}
		err := sp.ipcWriter.Write(rm.record)
		rm.record.Release()
		if err != nil {
			return nil, err
		}
		buf := sp.output.Bytes()

		// Reset the buffer
		sp.output.Reset()

		oapl[i] = &colarspb.OtlpArrowPayload{
			SubStreamId: sp.subStreamId,
			Type:        rm.payloadType,
			Record:      buf,
		}
	}

	batchId := fmt.Sprintf("%d", p.batchId)
	p.batchId++

	return &colarspb.BatchArrowRecords{
		BatchId:           batchId,
		OtlpArrowPayloads: oapl,
		DeliveryType:      deliveryType,
	}, nil
}
