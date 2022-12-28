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

package benchmark

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
)

type Profiler struct {
	batchSizes []int
	benchmarks []*ProfilerResult
	writer     io.Writer
	outputDir  string
	warmUpIter uint64
}

func NewProfiler(batchSizes []int, logfile string, warmUpIter uint64) *Profiler {
	if _, err := os.Stat(logfile); os.IsNotExist(err) {
		err = os.MkdirAll(path.Dir(logfile), 0700)
		if err != nil {
			log.Fatal("error creating directory: ", err)
		}
	}

	file, _ := os.OpenFile(filepath.Clean(logfile), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	mw := io.MultiWriter(os.Stdout, file)
	dt := time.Now()
	_, _ = fmt.Fprintln(mw, "\n================================================================================")
	_, _ = fmt.Fprintln(mw, "Benchmark started at: ", dt.String())
	_, _ = fmt.Fprintln(mw, "")

	return &Profiler{
		batchSizes: batchSizes,
		benchmarks: []*ProfilerResult{},
		writer:     mw,
		outputDir:  path.Dir(logfile),
		warmUpIter: warmUpIter,
	}
}

func (p *Profiler) Printf(format string, a ...any) {
	_, _ = fmt.Fprintf(p.writer, format, a...)
}

func (p *Profiler) Profile(profileable ProfileableSystem, maxIter uint64) error {
	runtime.GC()
	tags := strings.Join(profileable.Tags()[:], "+")

	p.benchmarks = append(p.benchmarks, &ProfilerResult{
		benchName: profileable.Name(),
		summaries: []BatchSummary{},
		tags:      tags,
	})

	for _, batchSize := range p.batchSizes {
		profileable.StartProfiling(p.writer)
		_, _ = fmt.Fprintf(p.writer, "Profiling '%s' (tags=[%v], batch-size=%d, dataset-size=%d)\n", profileable.Name(), strings.Join(profileable.Tags(), `,`), batchSize, profileable.DatasetSize())

		uncompressedSize := NewMetric()
		compressedSize := NewMetric()
		batchCeation := NewMetric()
		processing := NewMetric()
		serialization := NewMetric()
		deserialization := NewMetric()
		compression := NewMetric()
		decompression := NewMetric()
		totalTime := NewMetric()
		var processingResults []string

		profileable.InitBatchSize(p.writer, batchSize)

		for _i := uint64(0); _i < maxIter; _i++ {
			maxBatchCount := uint64(math.Ceil(float64(profileable.DatasetSize()) / float64(batchSize)))
			startAt := 0

			for batchNum := uint64(0); batchNum < maxBatchCount; batchNum++ {
				correctedBatchSize := min(profileable.DatasetSize()-startAt, batchSize)
				profileable.PrepareBatch(p.writer, startAt, correctedBatchSize)

				start := time.Now()

				// Batch creation
				profileable.CreateBatch(p.writer, startAt, correctedBatchSize)
				afterBatchCreation := time.Now()

				// Processing
				result := profileable.Process(p.writer)
				afterProcessing := time.Now()

				processingResults = append(processingResults, result)

				// Serialization
				buffers, err := profileable.Serialize(p.writer)
				if err != nil {
					return err
				}
				afterSerialization := time.Now()
				uncompressedSizeBytes := 0

				for _, buffer := range buffers {
					uncompressedSizeBytes += len(buffer)
				}
				if batchNum >= p.warmUpIter {
					uncompressedSize.Record(float64(uncompressedSizeBytes))
				}

				// Compression
				var compressedBuffers [][]byte
				for _, buffer := range buffers {
					compressedBuffer, err := profileable.CompressionAlgorithm().Compress(buffer)
					if err != nil {
						return err
					}

					compressedBuffers = append(compressedBuffers, compressedBuffer)
				}
				afterCompression := time.Now()
				compressedSizeBytes := 0
				for _, buffer := range compressedBuffers {
					compressedSizeBytes += len(buffer)
				}
				if batchNum >= p.warmUpIter {
					compressedSize.Record(float64(compressedSizeBytes))
				}

				// Decompression
				var uncompressedBuffers [][]byte

				for _, buffer := range compressedBuffers {
					uncompressedBuffer, err := profileable.CompressionAlgorithm().Decompress(buffer)
					if err != nil {
						return err
					}

					uncompressedBuffers = append(uncompressedBuffers, uncompressedBuffer)
				}
				afterDecompression := time.Now()

				if !bytesEqual(buffers, uncompressedBuffers) {
					return fmt.Errorf("buffers are not equal after decompression")
				}

				// Deserialization
				profileable.Deserialize(p.writer, buffers)

				afterDeserialization := time.Now()

				profileable.Clear()

				if batchNum >= p.warmUpIter {
					batchCeation.Record(afterBatchCreation.Sub(start).Seconds())
					processing.Record(afterProcessing.Sub(afterBatchCreation).Seconds())
					serialization.Record(afterSerialization.Sub(afterProcessing).Seconds())
					compression.Record(afterCompression.Sub(afterSerialization).Seconds())
					decompression.Record(afterDecompression.Sub(afterCompression).Seconds())
					deserialization.Record(afterDeserialization.Sub(afterDecompression).Seconds())

					totalTime.Record(
						afterBatchCreation.Sub(start).Seconds() +
							afterProcessing.Sub(afterBatchCreation).Seconds() +
							afterSerialization.Sub(afterProcessing).Seconds() +
							afterCompression.Sub(afterSerialization).Seconds() +
							afterDecompression.Sub(afterCompression).Seconds() +
							afterDeserialization.Sub(afterDecompression).Seconds(),
					)
				}
			}
		}

		profileable.ShowStats()
		currentBenchmark := p.benchmarks[len(p.benchmarks)-1]
		currentBenchmark.summaries = append(currentBenchmark.summaries, BatchSummary{
			batchSize:            batchSize,
			uncompressedSizeByte: uncompressedSize.ComputeSummary(),
			compressedSizeByte:   compressedSize.ComputeSummary(),
			batchCreationSec:     batchCeation.ComputeSummary(),
			processingSec:        processing.ComputeSummary(),
			serializationSec:     serialization.ComputeSummary(),
			deserializationSec:   deserialization.ComputeSummary(),
			compressionSec:       compression.ComputeSummary(),
			decompressionSec:     decompression.ComputeSummary(),
			totalTimeSec:         totalTime.ComputeSummary(),
			processingResults:    processingResults,
		})

		profileable.EndProfiling(p.writer)
	}

	return nil
}

func (p *Profiler) CheckProcessingResults() {
	for batchIdx := range p.batchSizes {
		if len(p.benchmarks) == 0 {
			continue
		}

		var refProcessingResults []string
		for _, benchmark := range p.benchmarks {
			if len(refProcessingResults) == 0 {
				refProcessingResults = benchmark.summaries[batchIdx].processingResults
			} else {
				if !stringsEqual(refProcessingResults, benchmark.summaries[batchIdx].processingResults) {
					panic("Processing results are not equal")
				}
			}
		}
	}
}

func (p *Profiler) PrintResults(maxIter uint64) {
	p.PrintStepsTiming(maxIter)
	p.PrintCompressionRatio(maxIter)
}

func (p *Profiler) PrintStepsTiming(_ uint64) {
	_, _ = fmt.Fprintf(p.writer, "\n")
	headers := []string{"Steps"}

	for _, benchmark := range p.benchmarks {
		headers = append(headers, fmt.Sprintf("%s %s - p99", benchmark.benchName, benchmark.tags))
	}

	table := tablewriter.NewWriter(p.writer)
	table.SetHeader(headers)
	table.SetBorder(false)
	headerColors := []tablewriter.Colors{tablewriter.Color(tablewriter.Normal, tablewriter.FgGreenColor)}

	for i := 0; i < len(p.benchmarks); i++ {
		headerColors = append(headerColors, tablewriter.Color())
	}

	table.SetHeaderColor(headerColors...)

	values := make(map[string]*Summary)

	for _, result := range p.benchmarks {
		for _, summary := range result.summaries {
			key := fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "batch_creation_sec")
			values[key] = summary.batchCreationSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "processing_sec")
			values[key] = summary.processingSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "serialization_sec")
			values[key] = summary.serializationSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "compression_sec")
			values[key] = summary.compressionSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "decompression_sec")
			values[key] = summary.decompressionSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "deserialization_sec")
			values[key] = summary.deserializationSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "total_time_sec")
			values[key] = summary.totalTimeSec
		}
	}

	transform := func(value float64) float64 { return value * 1000.0 }
	p.AddSection("Batch creation (ms)", "batch_creation_sec", table, values, transform)
	// p.AddSection("Batch processing (ms)", "processing_sec", table, values, transform)
	p.AddSection("Serialization (ms)", "serialization_sec", table, values, transform)
	p.AddSection("Compression (ms)", "compression_sec", table, values, transform)
	p.AddSection("Decompression (ms)", "decompression_sec", table, values, transform)
	p.AddSection("Deserialisation (ms)", "deserialization_sec", table, values, transform)
	p.AddSection("Total time (ms)", "total_time_sec", table, values, transform)

	table.Render()
}

func (p *Profiler) PrintCompressionRatio(maxIter uint64) {
	_, _ = fmt.Fprintf(p.writer, "\n")
	headers := []string{"Steps"}
	for _, benchmark := range p.benchmarks {
		headers = append(headers, fmt.Sprintf("%s %s - p99", benchmark.benchName, benchmark.tags))
	}

	table := tablewriter.NewWriter(p.writer)
	table.SetHeader(headers)
	table.SetBorder(false)
	headerColors := []tablewriter.Colors{tablewriter.Color(tablewriter.Normal, tablewriter.FgGreenColor)}

	for i := 0; i < len(p.benchmarks); i++ {
		headerColors = append(headerColors, tablewriter.Color())
	}

	table.SetHeaderColor(headerColors...)

	uncompressedTotal := make(map[string]int64)
	compressedTotal := make(map[string]int64)

	values := make(map[string]*Summary)

	for _, result := range p.benchmarks {
		for _, summary := range result.summaries {
			key := fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "compressed_size_byte")
			values[key] = summary.compressedSizeByte
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "total_compressed_size_byte")
			compressedTotal[key] = int64(summary.compressedSizeByte.Total(maxIter))
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "uncompressed_size_byte")
			values[key] = summary.uncompressedSizeByte
			uncompressedTotal[key] = int64(summary.uncompressedSizeByte.Total(maxIter))
		}
	}

	transform := func(value float64) float64 { return math.Trunc(value) }
	p.AddSectionWithTotal("Uncompressed size (bytes)", "uncompressed_size_byte", table, values, transform, maxIter)
	p.AddSectionWithTotal("Compressed size (bytes)", "compressed_size_byte", table, values, transform, maxIter)

	table.Render()
}

func (p *Profiler) AddSection(label string, step string, table *tablewriter.Table, values map[string]*Summary, transform func(float64) float64) {
	labels := []string{label}
	colors := []tablewriter.Colors{tablewriter.Color(tablewriter.Normal, tablewriter.FgGreenColor)}
	for i := 0; i < len(p.benchmarks); i++ {
		labels = append(labels, "")
		colors = append(colors, tablewriter.Color())
	}
	table.Rich(labels, colors)

	for _, batchSize := range p.batchSizes {
		row := []string{fmt.Sprintf("batch_size: %d", batchSize)}
		refImplName := ""
		for _, result := range p.benchmarks {
			key := fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, batchSize, step)
			improvement := ""

			if refImplName == "" {
				refImplName = fmt.Sprintf("%s:%s", result.benchName, result.tags)
			} else {
				refKey := fmt.Sprintf("%s:%d:%s", refImplName, batchSize, step)
				improvement = fmt.Sprintf("(x%.2f)", values[refKey].P99/values[key].P99)
			}

			value := transform(values[key].P99)
			if value == math.Trunc(value) {
				row = append(row, fmt.Sprintf("%d %s", int64(value), improvement))
			} else {
				if value >= 1.0 {
					row = append(row, fmt.Sprintf("%.3f %s", value, improvement))
				} else {
					row = append(row, fmt.Sprintf("%.5f %s", value, improvement))
				}
			}
		}

		table.Append(row)
	}
}

func (p *Profiler) AddSectionWithTotal(label string, step string, table *tablewriter.Table, values map[string]*Summary, transform func(float64) float64, maxIter uint64) {
	labels := []string{label}
	colors := []tablewriter.Colors{tablewriter.Color(tablewriter.Normal, tablewriter.FgGreenColor)}
	for i := 0; i < len(p.benchmarks); i++ {
		labels = append(labels, "")
		colors = append(colors, tablewriter.Color())
	}
	table.Rich(labels, colors)

	for _, batchSize := range p.batchSizes {
		row := []string{fmt.Sprintf("batch_size: %d", batchSize)}
		refImplName := ""
		for _, result := range p.benchmarks {
			key := fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, batchSize, step)
			improvement := ""

			if refImplName == "" {
				refImplName = fmt.Sprintf("%s:%s", result.benchName, result.tags)
			} else {
				refKey := fmt.Sprintf("%s:%d:%s", refImplName, batchSize, step)
				improvement = fmt.Sprintf("(x%.2f)", values[refKey].P99/values[key].P99)
			}

			value := transform(values[key].P99)
			if value == math.Trunc(value) {
				accumulatedSize := uint64(values[key].Total(maxIter))
				row = append(row, fmt.Sprintf("%d %s (total: %s)", int64(value), improvement, humanize.Bytes(accumulatedSize)))
			} else {
				if value >= 1.0 {
					row = append(row, fmt.Sprintf("%.3f %s (total: %s)", value, improvement, humanize.Bytes(uint64(values[key].Total(maxIter)))))
				} else {
					row = append(row, fmt.Sprintf("%.5f %s (total: %s)", value, improvement, humanize.Bytes(uint64(values[key].Total(maxIter)))))
				}
			}
		}

		table.Append(row)
	}
}

func (p *Profiler) ExportMetricsTimesCSV(filePrefix string) {
	filename := fmt.Sprintf("%s/%s_times.csv", p.outputDir, filePrefix)
	file, err := os.OpenFile(filepath.Clean(filename), os.O_CREATE|os.O_WRONLY, 0600)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	dataWriter := bufio.NewWriter(file)

	_, err = dataWriter.WriteString("batch_size,duration_ms,protocol,step\n")
	if err != nil {
		panic(fmt.Sprintf("failed writing to file: %s", err))
	}

	for batchIdx, batchSize := range p.batchSizes {
		if len(p.benchmarks) == 0 {
			continue
		}

		for _, result := range p.benchmarks {
			batchCreationMs := result.summaries[batchIdx].batchCreationSec.P99
			serializationMs := result.summaries[batchIdx].serializationSec.P99
			compressionMs := result.summaries[batchIdx].compressionSec.P99
			decompressionMs := result.summaries[batchIdx].decompressionSec.P99
			deserializationMs := result.summaries[batchIdx].deserializationSec.P99

			_, err = dataWriter.WriteString(fmt.Sprintf("%d,%f,%s [%s],0_Batch_creation\n", batchSize, batchCreationMs, result.benchName, result.tags))
			if err != nil {
				panic(fmt.Sprintf("failed writing to file: %s", err))
			}

			_, err = dataWriter.WriteString(fmt.Sprintf("%d,%f,%s [%s],1_Serialization\n", batchSize, serializationMs, result.benchName, result.tags))
			if err != nil {
				panic(fmt.Sprintf("failed writing to file: %s", err))
			}

			_, err = dataWriter.WriteString(fmt.Sprintf("%d,%f,%s [%s],2_Compression\n", batchSize, compressionMs, result.benchName, result.tags))
			if err != nil {
				panic(fmt.Sprintf("failed writing to file: %s", err))
			}

			_, err = dataWriter.WriteString(fmt.Sprintf("%d,%f,%s [%s],3_Decompression\n", batchSize, decompressionMs, result.benchName, result.tags))
			if err != nil {
				panic(fmt.Sprintf("failed writing to file: %s", err))
			}

			_, err = dataWriter.WriteString(fmt.Sprintf("%d,%f,%s [%s],4_Deserialization\n", batchSize, deserializationMs, result.benchName, result.tags))
			if err != nil {
				panic(fmt.Sprintf("failed writing to file: %s", err))
			}
		}
	}

	err = dataWriter.Flush()
	if err != nil {
		panic(fmt.Sprintf("failed flushing the file: %s", err))
	}

	err = file.Close()
	if err != nil {
		panic(fmt.Sprintf("failed closing the file: %s", err))
	}

	_, _ = fmt.Fprintf(p.writer, "Time meseasurements exported to %s\n", filename)
}

func (p *Profiler) ExportMetricsBytesCSV(filePrefix string) {
	filename := fmt.Sprintf("%s/%s_bytes.csv", p.outputDir, filePrefix)
	file, err := os.OpenFile(filepath.Clean(filename), os.O_CREATE|os.O_WRONLY, 0600)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	dataWriter := bufio.NewWriter(file)

	_, err = dataWriter.WriteString("batch_size,iteration,compressed_size_byte,uncompressed_size_byte,Protocol\n")
	if err != nil {
		panic(fmt.Sprintf("failed writing to file: %s", err))
	}

	for batchIdx, batchSize := range p.batchSizes {
		if len(p.benchmarks) == 0 {
			continue
		}

		numSamples := len(p.benchmarks[0].summaries[batchIdx].batchCreationSec.Values)
		for sampleIdx := 0; sampleIdx < numSamples; sampleIdx++ {
			for _, result := range p.benchmarks {
				line := fmt.Sprintf("%d,%d", batchSize, sampleIdx)
				compressedSizeByte := result.summaries[batchIdx].compressedSizeByte.Values[sampleIdx]
				uncompressedSizeByte := result.summaries[batchIdx].uncompressedSizeByte.Values[sampleIdx]

				line += fmt.Sprintf(",%f,%f,%s [%s]\n", compressedSizeByte, uncompressedSizeByte, result.benchName, result.tags)

				_, err = dataWriter.WriteString(line)
				if err != nil {
					panic(fmt.Sprintf("failed writing to file: %s", err))
				}
			}
		}
	}

	err = dataWriter.Flush()
	if err != nil {
		panic(fmt.Sprintf("failed flushing the file: %s", err))
	}
	err = file.Close()
	if err != nil {
		panic(fmt.Sprintf("failed closing the file: %s", err))
	}

	_, _ = fmt.Fprintf(p.writer, "Meseasurements of the message sizes exported to %s\n", filename)
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func bytesEqual(buffers1, buffers2 [][]byte) bool {
	if len(buffers1) != len(buffers2) {
		return false
	}

	for i := range buffers1 {
		if !bytes.Equal(buffers1[i], buffers2[i]) {
			return false
		}
	}

	return true
}

func stringsEqual(buffers1, buffers2 []string) bool {
	if len(buffers1) != len(buffers2) {
		return false
	}
	for i, v1 := range buffers1 {
		if v1 != buffers2[i] {
			return false
		}
	}
	return true
}
