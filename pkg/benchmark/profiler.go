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

// Section identifiers used in the benchmark output.
var (
	BatchCreationSection    = NewSectionConfig("batch_creation_sec", "Proto msg creation (ms)")
	SerializationSection    = NewSectionConfig("serialization_sec", "Serialization (ms)")
	CompressionSection      = NewSectionConfig("compression_sec", "Compression (ms)")
	DecompressionSection    = NewSectionConfig("decompression_sec", "Decompression (ms)")
	DeserializationSection  = NewSectionConfig("deserialization_sec", "Deserialisation (ms)")
	TotalTimeSection        = NewSectionConfig("total_time_sec", "Total time (ms)")
	ProcessingSection       = NewSectionConfig("processing_sec", "Batch processing (ms)")
	UncompressedSizeSection = NewSectionConfig("uncompressed_size", "Uncompressed size (bytes)")
	CompressedSizeSection   = NewSectionConfig("compressed_size", "Compressed size (bytes)")
)

// Profiler is the main profiler object used to implement benchmarks.
type Profiler struct {
	batchSizes []int
	benchmarks []*ProfilerResult
	writer     io.Writer
	outputDir  string
	warmUpIter uint64
}

// SectionConfig is the configuration for a section of the benchmark table output.
type SectionConfig struct {
	ID string

	Title string

	// Default column configuration (if not overridden by a custom column config).
	defaultColumnConfig *ColumnConfig

	// Custom column configuration per column identifier.
	columns map[string]*ColumnConfig
}

// ColumnConfig is the configuration for a column of the benchmark table output.
type ColumnConfig struct {
	subTitle         string
	metricApplicable bool
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
	colorReset := "\033[0m"
	colorCyan := "\033[36m"
	colorYellow := "\033[33m"

	// Time spent in each step + notes
	println()
	println(colorCyan)
	println("======= Measurement of the time spent on the different steps for each protocol configuration ========", colorReset)
	p.PrintStepsTiming(maxIter)

	// Message size and compression ratio
	println()
	println(colorCyan)
	println("======== Measurement of message size and compression ratio for each protocol configuration ==========", colorReset)
	p.PrintCompressionRatio(maxIter)

	// Notes on OTLP Arrow column
	println()
	println(colorYellow, "Notes for the OTLP Arrow column", colorReset)
	println("  - The `Proto msg creation` step represents the creation of the entire BatchArrowRecords, including:")
	println("    creating the Arrow Record, encoding it, compressing it, and embedding it in the BatchArrowRecords")
	println("    protobuf message. The compression is performed internally by the Arrow lib during the encoding of")
	println("    the message and therefore cannot be measured separately.")
	println("  - The `Serialization`/`Deserialization` steps are applied to the BatchArrowRecords protobuf message")
	println("    which is composed of a very small number of fields explaining the huge speed difference with  the")
	println("    equivalent OTLP steps. Due to the zero-copy nature of Arrow, the deserialization of Arrow Records")
	println("    does not exist. The decompression of the Arrow Record is included in the deserialization step for")
	println("    the reasons mentioned in the previous point.")
	println("  - The `Uncompressed size (bytes)` step is not available for Arrow OTLP for the reasons mentioned in")
	println("    the first point.")
	println()
}

func (p *Profiler) PrintStepsTiming(_ uint64) {
	_, _ = fmt.Fprintf(p.writer, "\n")
	headers := []string{"Main steps"}

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
			key := fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, BatchCreationSection.ID)
			values[key] = summary.batchCreationSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, ProcessingSection.ID)
			values[key] = summary.processingSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, SerializationSection.ID)
			values[key] = summary.serializationSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, CompressionSection.ID)
			values[key] = summary.compressionSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, DecompressionSection.ID)
			values[key] = summary.decompressionSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, DeserializationSection.ID)
			values[key] = summary.deserializationSec
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, TotalTimeSection.ID)
			values[key] = summary.totalTimeSec
		}
	}

	transform := func(value float64) float64 { return value * 1000.0 }
	p.AddSection(BatchCreationSection, table, values, transform)
	// p.AddSection(ProcessingSection, table, values, transform)
	p.AddSection(SerializationSection, table, values, transform)
	p.AddSection(CompressionSection, table, values, transform)
	p.AddSection(DecompressionSection, table, values, transform)
	p.AddSection(DeserializationSection, table, values, transform)
	p.AddSection(TotalTimeSection, table, values, transform)

	table.Render()
}

func (p *Profiler) PrintCompressionRatio(maxIter uint64) {
	_, _ = fmt.Fprintf(p.writer, "\n")
	headers := []string{"Batch message (avg sz)"}
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
			key := fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, CompressedSizeSection.ID)
			values[key] = summary.compressedSizeByte
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, "total_compressed_size_byte")
			compressedTotal[key] = int64(summary.compressedSizeByte.Total(maxIter))
			key = fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, summary.batchSize, UncompressedSizeSection.ID)
			values[key] = summary.uncompressedSizeByte
			uncompressedTotal[key] = int64(summary.uncompressedSizeByte.Total(maxIter))
		}
	}

	transform := func(value float64) float64 { return math.Trunc(value) }
	p.AddSectionWithTotal(UncompressedSizeSection, table, values, transform, maxIter)
	p.AddSectionWithTotal(CompressedSizeSection, table, values, transform, maxIter)

	table.Render()
}

func (p *Profiler) AddSection(section *SectionConfig, table *tablewriter.Table, values map[string]*Summary, transform func(float64) float64) {
	titles := []string{section.Title}
	colors := []tablewriter.Colors{tablewriter.Color(tablewriter.Normal, tablewriter.FgGreenColor)}
	for i := 0; i < len(p.benchmarks); i++ {
		result := p.benchmarks[i]
		titles = append(titles, section.SubTitle(fmt.Sprintf("%s:%s", result.benchName, result.tags)))
		colors = append(colors, tablewriter.Color())
	}
	table.Rich(titles, colors)

	for _, batchSize := range p.batchSizes {
		row := []string{fmt.Sprintf("batch_size: %d", batchSize)}
		refImplName := ""
		for _, result := range p.benchmarks {
			metricNotApplicable := section.MetricNotApplicable(fmt.Sprintf("%s:%s", result.benchName, result.tags))
			key := fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, batchSize, section.ID)
			improvement := ""

			if refImplName == "" {
				refImplName = fmt.Sprintf("%s:%s", result.benchName, result.tags)
			} else {
				refKey := fmt.Sprintf("%s:%d:%s", refImplName, batchSize, section.ID)
				improvement = fmt.Sprintf("(x%.2f)", values[refKey].P99/values[key].P99)
			}

			value := transform(values[key].P99)
			decoratedValue := "Not Applicable"
			if value == math.Trunc(value) {
				if metricNotApplicable {
					decoratedValue = fmt.Sprintf("%d %s", int64(value), improvement)
				}
				row = append(row, decoratedValue)
			} else {
				if value >= 1.0 {
					if metricNotApplicable {
						decoratedValue = fmt.Sprintf("%.3f %s", value, improvement)
					}
					row = append(row, decoratedValue)
				} else {
					if metricNotApplicable {
						decoratedValue = fmt.Sprintf("%.5f %s", value, improvement)
					}
					row = append(row, decoratedValue)
				}
			}
		}

		table.Append(row)
	}
}

func (p *Profiler) AddSectionWithTotal(section *SectionConfig, table *tablewriter.Table, values map[string]*Summary, transform func(float64) float64, maxIter uint64) {
	labels := []string{section.Title}
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
			key := fmt.Sprintf("%s:%s:%d:%s", result.benchName, result.tags, batchSize, section.ID)
			metricNotApplicable := section.MetricNotApplicable(fmt.Sprintf("%s:%s", result.benchName, result.tags))
			improvement := ""

			if refImplName == "" {
				refImplName = fmt.Sprintf("%s:%s", result.benchName, result.tags)
			} else {
				refKey := fmt.Sprintf("%s:%d:%s", refImplName, batchSize, section.ID)
				improvement = fmt.Sprintf("(x%.2f)", values[refKey].P99/values[key].P99)
			}

			value := transform(values[key].P99)
			decoratedValue := "Not Applicable"
			if value == math.Trunc(value) {
				accumulatedSize := uint64(values[key].Total(maxIter))
				if metricNotApplicable {
					decoratedValue = fmt.Sprintf("%d %s (total: %s)", int64(value), improvement, humanize.Bytes(accumulatedSize))
				}
				row = append(row, decoratedValue)
			} else {
				if value >= 1.0 {
					if metricNotApplicable {
						decoratedValue = fmt.Sprintf("%.3f %s (total: %s)", value, improvement, humanize.Bytes(uint64(values[key].Total(maxIter))))
					}
					row = append(row, decoratedValue)
				} else {
					if metricNotApplicable {
						decoratedValue = fmt.Sprintf("%.5f %s (total: %s)", value, improvement, humanize.Bytes(uint64(values[key].Total(maxIter))))
					}
					row = append(row, decoratedValue)
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

// DefaultColumnConfig creates a default column config.
func DefaultColumnConfig() *ColumnConfig {
	return &ColumnConfig{
		subTitle:         "",
		metricApplicable: true,
	}
}

// NewSectionConfig creates a new SectionConfig with default values.
func NewSectionConfig(sectionID string, title string) *SectionConfig {
	return &SectionConfig{
		ID:                  sectionID,
		Title:               title,
		defaultColumnConfig: DefaultColumnConfig(),
		columns:             make(map[string]*ColumnConfig),
	}
}

func (sc *SectionConfig) CustomColumnFor(ps ProfileableSystem) *ColumnConfig {
	columnID := ProfileableSystemID(ps)
	column, ok := sc.columns[columnID]

	if !ok {
		column = DefaultColumnConfig()
		sc.columns[columnID] = column
	}

	return column
}

func (sc *SectionConfig) SubTitle(columnID string) string {
	column, ok := sc.columns[columnID]
	if !ok {
		return sc.defaultColumnConfig.subTitle
	}
	return column.subTitle
}

func (sc *SectionConfig) MetricNotApplicable(columnID string) bool {
	column, ok := sc.columns[columnID]
	if !ok {
		return sc.defaultColumnConfig.metricApplicable
	}
	return column.metricApplicable
}

func (c *ColumnConfig) SubTitle(subTitle string) *ColumnConfig {
	c.subTitle = subTitle
	return c
}

func (c *ColumnConfig) MetricNotApplicable() *ColumnConfig {
	c.metricApplicable = false
	return c
}
