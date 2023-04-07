/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package stats

import (
	"fmt"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// ProducerStats is a struct that contains stats about the OTLP Arrow Producer.
type ProducerStats struct {
	MetricsBatchesProduced uint64
	LogsBatchesProduced    uint64
	TracesBatchesProduced  uint64
	StreamProducersCreated uint64
	StreamProducersClosed  uint64
	RecordBuilderStats     RecordBuilderStats

	SchemaStatsEnabled bool
}

type RecordBuilderStats struct {
	SchemaUpdatesPerformed     uint64
	DictionaryIndexTypeChanged uint64
	DictionaryOverflowDetected uint64
	DictMigrationStats         DictionaryMigrationStats
}

// DictionaryMigrationStats is a struct that contains stats about the migration
// of dictionaries between RecordBuilder instances in the context of a schema
// update.
type DictionaryMigrationStats struct {
	// Histogram of string dictionary size
	StringDictSz *hdrhistogram.Histogram
	// Histogram of binary dictionary size
	BinaryDictSz *hdrhistogram.Histogram
	// Histogram of fixed size binary dictionary size
	FixedSizeBinaryDictSz *hdrhistogram.Histogram
	// Histogram of int32 dictionary size
	Int32DictSz *hdrhistogram.Histogram
	// Histogram of uint32 dictionary size
	Uint32DictSz *hdrhistogram.Histogram
}

// NewProducerStats creates a new ProducerStats struct.
func NewProducerStats() *ProducerStats {
	return &ProducerStats{
		MetricsBatchesProduced: 0,
		LogsBatchesProduced:    0,
		TracesBatchesProduced:  0,
		StreamProducersCreated: 0,
		StreamProducersClosed:  0,
		RecordBuilderStats: RecordBuilderStats{
			SchemaUpdatesPerformed:     0,
			DictionaryIndexTypeChanged: 0,
			DictionaryOverflowDetected: 0,
			DictMigrationStats: DictionaryMigrationStats{
				StringDictSz:          hdrhistogram.New(1, 1000000, 2),
				BinaryDictSz:          hdrhistogram.New(1, 1000000, 2),
				FixedSizeBinaryDictSz: hdrhistogram.New(1, 1000000, 2),
				Int32DictSz:           hdrhistogram.New(1, 1000000, 2),
				Uint32DictSz:          hdrhistogram.New(1, 1000000, 2),
			},
		},
		SchemaStatsEnabled: false,
	}
}

// GetAndReset returns the current stats and resets them to zero.
func (s *ProducerStats) GetAndReset() ProducerStats {
	stats := *s
	s.Reset()
	return stats
}

// Reset sets all stats to zero.
func (s *ProducerStats) Reset() {
	s.MetricsBatchesProduced = 0
	s.LogsBatchesProduced = 0
	s.TracesBatchesProduced = 0
	s.StreamProducersCreated = 0
	s.StreamProducersClosed = 0
	s.RecordBuilderStats.Reset()
}

// Reset sets all stats to zero.
func (s *RecordBuilderStats) Reset() {
	s.SchemaUpdatesPerformed = 0
	s.DictionaryIndexTypeChanged = 0
	s.DictionaryOverflowDetected = 0
	s.DictMigrationStats.Reset()
}

// Reset sets all stats to zero.
func (s *DictionaryMigrationStats) Reset() {
	s.StringDictSz.Reset()
	s.BinaryDictSz.Reset()
	s.FixedSizeBinaryDictSz.Reset()
	s.Int32DictSz.Reset()
	s.Uint32DictSz.Reset()
}

// Show prints the stats to the console.
func (s *ProducerStats) Show(indent string) {
	fmt.Printf("%s- Metrics batches produced: %d\n", indent, s.MetricsBatchesProduced)
	fmt.Printf("%s- Logs batches produced: %d\n", indent, s.LogsBatchesProduced)
	fmt.Printf("%s- Traces batches produced: %d\n", indent, s.TracesBatchesProduced)
	fmt.Printf("%s- Stream producers created: %d\n", indent, s.StreamProducersCreated)
	fmt.Printf("%s- Stream producers closed: %d\n", indent, s.StreamProducersClosed)
	fmt.Printf("%s- RecordBuilder:\n", indent)
	s.RecordBuilderStats.Show(indent + "  ")
}

// Show prints the RecordBuilder stats to the console.
func (s *RecordBuilderStats) Show(indent string) {
	fmt.Printf("%s- Schema updates performed: %d\n", indent, s.SchemaUpdatesPerformed)
	fmt.Printf("%s- Dictionary index type changed: %d\n", indent, s.DictionaryIndexTypeChanged)
	fmt.Printf("%s- Dictionary overflow detected: %d\n", indent, s.DictionaryOverflowDetected)
	fmt.Printf("%s- Dictionary migration stats:\n", indent)
	s.DictMigrationStats.Show(indent + "  ")
}

// Show prints the DictionaryMigrationStats to the console.
func (s *DictionaryMigrationStats) Show(indent string) {
	if s.StringDictSz.TotalCount() > 0 {
		fmt.Printf("%s- String dictionary size (count, p50, p90, p99): %d, %d, %d, %d\n",
			indent,
			s.StringDictSz.TotalCount(),
			s.StringDictSz.ValueAtQuantile(50),
			s.StringDictSz.ValueAtQuantile(90),
			s.StringDictSz.ValueAtQuantile(99))
	}
	if s.BinaryDictSz.TotalCount() > 0 {
		fmt.Printf("%s- Binary dictionary size (count, p50, p90, p99): %d, %d, %d, %d\n",
			indent,
			s.BinaryDictSz.TotalCount(),
			s.BinaryDictSz.ValueAtQuantile(50),
			s.BinaryDictSz.ValueAtQuantile(90),
			s.BinaryDictSz.ValueAtQuantile(99))
	}
	if s.FixedSizeBinaryDictSz.TotalCount() > 0 {
		fmt.Printf("%s- Fixed size binary dictionary size (count, p50, p90, p99): %d, %d, %d, %d\n",
			indent,
			s.FixedSizeBinaryDictSz.TotalCount(),
			s.FixedSizeBinaryDictSz.ValueAtQuantile(50),
			s.FixedSizeBinaryDictSz.ValueAtQuantile(90),
			s.FixedSizeBinaryDictSz.ValueAtQuantile(99))
	}
	if s.Int32DictSz.TotalCount() > 0 {
		fmt.Printf("%s- Int32 dictionary size (count, p50, p90, p99): %d, %d, %d, %d\n",
			indent,
			s.Int32DictSz.TotalCount(),
			s.Int32DictSz.ValueAtQuantile(50),
			s.Int32DictSz.ValueAtQuantile(90),
			s.Int32DictSz.ValueAtQuantile(99))
	}
	if s.Uint32DictSz.TotalCount() > 0 {
		fmt.Printf("%s- Uint32 dictionary size (count, p50, p90, p99): %d, %d, %d, %d\n",
			indent,
			s.Uint32DictSz.TotalCount(),
			s.Uint32DictSz.ValueAtQuantile(50),
			s.Uint32DictSz.ValueAtQuantile(90),
			s.Uint32DictSz.ValueAtQuantile(99))
	}
}
