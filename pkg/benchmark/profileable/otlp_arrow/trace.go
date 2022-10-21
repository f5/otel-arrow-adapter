package otlp_arrow

import (
	"io"

	"google.golang.org/protobuf/proto"

	v1 "github.com/lquerel/otel-arrow-adapter/api/collector/arrow/v1"
	"github.com/lquerel/otel-arrow-adapter/pkg/air/config"
	"github.com/lquerel/otel-arrow-adapter/pkg/benchmark"
	"github.com/lquerel/otel-arrow-adapter/pkg/benchmark/dataset"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/arrow_record"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/traces"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type TraceProfileable struct {
	tags              []string
	compression       benchmark.CompressionAlgorithm
	dataset           dataset.TraceDataset
	traces            []ptrace.Traces
	otlpArrowProducer *traces.OtlpArrowProducer
	producer          *arrow_record.Producer
	consumer          *arrow_record.Consumer
	batchArrowRecords []*v1.BatchArrowRecords
	config            *config.Config
}

func NewTraceProfileable(tags []string, dataset dataset.TraceDataset, config *config.Config, compression benchmark.CompressionAlgorithm) *TraceProfileable {
	return &TraceProfileable{
		tags:              tags,
		dataset:           dataset,
		compression:       compression,
		otlpArrowProducer: nil,
		producer:          arrow_record.NewProducerWithConfig(config),
		consumer:          arrow_record.NewConsumer(),
		batchArrowRecords: make([]*v1.BatchArrowRecords, 0, 10),
		config:            config,
	}
}

func (s *TraceProfileable) Name() string {
	return "OTLP_ARROW"
}

func (s *TraceProfileable) Tags() []string {
	tags := []string{s.compression.String()}
	tags = append(tags, s.tags...)
	return tags
}
func (s *TraceProfileable) DatasetSize() int { return s.dataset.Len() }
func (s *TraceProfileable) CompressionAlgorithm() benchmark.CompressionAlgorithm {
	return s.compression
}
func (s *TraceProfileable) StartProfiling(_ io.Writer) {
	s.otlpArrowProducer = traces.NewOtlpArrowProducerWith(s.config)
}
func (s *TraceProfileable) EndProfiling(writer io.Writer) {
	s.otlpArrowProducer.DumpMetadata(writer)
	s.otlpArrowProducer = nil
}
func (s *TraceProfileable) InitBatchSize(_ io.Writer, _ int) {}
func (s *TraceProfileable) PrepareBatch(_ io.Writer, startAt, size int) {
	s.traces = s.dataset.Traces(startAt, size)
}
func (s *TraceProfileable) CreateBatch(_ io.Writer, _, _ int) {
	// Conversion of OTLP metrics to OTLP Arrow Records
	s.batchArrowRecords = make([]*v1.BatchArrowRecords, 0, len(s.traces))
	for _, traceReq := range s.traces {
		bar, err := s.producer.BatchArrowRecordsFrom(traceReq)
		if err != nil {
			panic(err)
		}
		s.batchArrowRecords = append(s.batchArrowRecords, bar)
	}
}
func (s *TraceProfileable) Process(io.Writer) string {
	// Not used in this benchmark
	return ""
}
func (s *TraceProfileable) Serialize(io.Writer) ([][]byte, error) {
	buffers := make([][]byte, len(s.batchArrowRecords))
	for i, be := range s.batchArrowRecords {
		bytes, err := proto.Marshal(be)
		if err != nil {
			return nil, err
		}
		buffers[i] = bytes
	}
	return buffers, nil
}

func (s *TraceProfileable) Deserialize(_ io.Writer, buffers [][]byte) {
	s.batchArrowRecords = make([]*v1.BatchArrowRecords, len(buffers))
	for i, b := range buffers {
		be := &v1.BatchArrowRecords{}
		if err := proto.Unmarshal(b, be); err != nil {
			panic(err)
		}
		s.batchArrowRecords[i] = be

		// ToDo TMP
		//ibes, err := s.consumer.Consume(be)
		//if err != nil {
		//	panic(err)
		//}
		//for _, ibe := range ibes {
		//	request, err := trace2.ArrowRecordToOtlpTraces(ibe.Record())
		//	if err != nil {
		//		panic(err)
		//	}
		//	if len(request.ResourceSpans) == 0 {
		//		panic("no resource spans")
		//	}
		//}
	}
}
func (s *TraceProfileable) Clear() {
	s.traces = nil
	s.batchArrowRecords = s.batchArrowRecords[:0]
}
func (s *TraceProfileable) ShowStats() {}
