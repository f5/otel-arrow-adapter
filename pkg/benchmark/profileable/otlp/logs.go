package otlp

import (
	"io"

	"google.golang.org/protobuf/proto"

	v1 "go.opentelemetry.io/collector/pdata/plog"
	"otel-arrow-adapter/pkg/benchmark"
	"otel-arrow-adapter/pkg/benchmark/dataset"
)

type LogsProfileable struct {
	compression benchmark.CompressionAlgorithm
	dataset     dataset.LogsDataset
	logs        []*v1.Logs
}

func NewLogsProfileable(dataset dataset.LogsDataset, compression benchmark.CompressionAlgorithm) *LogsProfileable {
	return &LogsProfileable{dataset: dataset, compression: compression}
}

func (s *LogsProfileable) Name() string {
	return "OTLP"
}

func (s *LogsProfileable) Tags() []string {
	return []string{s.compression.String()}
}
func (s *LogsProfileable) DatasetSize() int { return s.dataset.Len() }
func (s *LogsProfileable) CompressionAlgorithm() benchmark.CompressionAlgorithm {
	return s.compression
}
func (s *LogsProfileable) StartProfiling(io.Writer)           {}
func (s *LogsProfileable) EndProfiling(io.Writer)             {}
func (s *LogsProfileable) InitBatchSize(_ io.Writer, _ int)   {}
func (s *LogsProfileable) PrepareBatch(_ io.Writer, _, _ int) {}
func (s *LogsProfileable) CreateBatch(_ io.Writer, startAt, size int) {
	s.logs = s.dataset.Logs(startAt, size)
}
func (s *LogsProfileable) Process(io.Writer) string { return "" }
func (s *LogsProfileable) Serialize(io.Writer) ([][]byte, error) {
	buffers := make([][]byte, len(s.logs))
	for i, m := range s.logs {
		bytes, err := proto.Marshal(m)
		if err != nil {
			return nil, err
		}
		buffers[i] = bytes
	}
	return buffers, nil
}
func (s *LogsProfileable) Deserialize(_ io.Writer, buffers [][]byte) {
	s.logs = make([]*v1.Logs, len(buffers))
	for i, b := range buffers {
		m := &v1.Logs{}
		if err := proto.Unmarshal(b, m); err != nil {
			panic(err)
		}
		s.logs[i] = m
	}
}
func (s *LogsProfileable) Clear() {
	s.logs = nil
}
func (s *LogsProfileable) ShowStats() {}
