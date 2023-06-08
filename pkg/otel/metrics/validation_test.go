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

package metrics

import (
	"encoding/json"
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/f5/otel-arrow-adapter/pkg/config"
	"github.com/f5/otel-arrow-adapter/pkg/datagen"
	"github.com/f5/otel-arrow-adapter/pkg/otel/assert"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	cfg "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/config"
	ametrics "github.com/f5/otel-arrow-adapter/pkg/otel/metrics/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/metrics/otlp"
	"github.com/f5/otel-arrow-adapter/pkg/otel/stats"
	"github.com/f5/otel-arrow-adapter/pkg/record_message"
)

var DefaultDictConfig = cfg.NewDictionary(math.MaxUint16)

// TestBackAndForthConversion tests the conversion of OTLP metrics to Arrow and back to OTLP.
// The initial OTLP metrics are generated from a synthetic dataset.
// This test is based on the JSON serialization of the initial generated OTLP metrics compared to the JSON serialization
// of the OTLP metrics generated from the Arrow records.
func TestBackAndForthConversion(t *testing.T) {
	t.Parallel()

	metricsGen := MetricsGenerator()
	expectedRequest := pmetricotlp.NewExportRequestFromMetrics(metricsGen.GenerateAllKindOfMetrics(100, 100))

	GenericMetricTests(t, expectedRequest)
	MultiRoundOfMessUpArrowRecordsTests(t, expectedRequest)
}

func TestSums(t *testing.T) {
	t.Parallel()

	metricsGen := MetricsGenerator()
	expectedRequest := pmetricotlp.NewExportRequestFromMetrics(metricsGen.GenerateSums(100, 100))

	GenericMetricTests(t, expectedRequest)
	MultiRoundOfMessUpArrowRecordsTests(t, expectedRequest)
}

func TestExponentialHistograms(t *testing.T) {
	t.Parallel()

	metricsGen := MetricsGenerator()
	expectedRequest := pmetricotlp.NewExportRequestFromMetrics(metricsGen.GenerateExponentialHistograms(100, 100))

	GenericMetricTests(t, expectedRequest)
	MultiRoundOfMessUpArrowRecordsTests(t, expectedRequest)
}

func MetricsGenerator() *datagen.MetricsGenerator {
	entropy := datagen.NewTestEntropy(int64(rand.Uint64())) //nolint:gosec // only used for testing

	dg := datagen.NewDataGenerator(entropy, entropy.NewStandardResourceAttributes(), entropy.NewStandardInstrumentationScopes()).
		WithConfig(datagen.Config{
			ProbMetricDescription: 0.5,
			ProbMetricUnit:        0.5,
			ProbHistogramHasSum:   0.5,
			ProbHistogramHasMin:   0.5,
			ProbHistogramHasMax:   0.5,
		})
	return datagen.NewMetricsGeneratorWithDataGenerator(dg)
}

func GenericMetricTests(t *testing.T, expectedRequest pmetricotlp.ExportRequest) {
	// Convert the OTLP metrics request to Arrow.
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	rBuilder := builder.NewRecordBuilderExt(pool, ametrics.MetricsSchema, DefaultDictConfig, stats.NewProducerStats())
	defer rBuilder.Release()

	var record arrow.Record
	var relatedRecords []*record_message.RecordMessage

	conf := config.DefaultConfig()

	for {
		lb, err := ametrics.NewMetricsBuilder(rBuilder, ametrics.NewConfig(conf), stats.NewProducerStats())
		require.NoError(t, err)
		defer lb.Release()

		err = lb.Append(expectedRequest.Metrics())
		require.NoError(t, err)

		record, err = rBuilder.NewRecord()
		if err == nil {
			relatedRecords, err = lb.RelatedData().BuildRecordMessages()
			require.NoError(t, err)
			break
		}
		require.Error(t, schema.ErrSchemaNotUpToDate)
	}

	relatedData, _, err := otlp.RelatedDataFrom(relatedRecords)
	require.NoError(t, err)

	// Convert the Arrow records back to OTLP.
	metrics, err := otlp.MetricsFrom(record, relatedData)
	require.NoError(t, err)

	record.Release()

	assert.Equiv(t, []json.Marshaler{expectedRequest}, []json.Marshaler{pmetricotlp.NewExportRequestFromMetrics(metrics)})
}

// MultiRoundOfMessUpArrowRecordsTests tests the robustness of the conversion of
// OTel Arrow records to OTLP metrics. These tests should never trigger a panic.
// For every main record, and related records (if any), we mix up the Arrow
// records in order to test the robustness of the conversion. In this situation,
// the conversion can generate errors, but should never panic.
func MultiRoundOfMessUpArrowRecordsTests(t *testing.T, expectedRequest pmetricotlp.ExportRequest) {
	rng := rand.New(rand.NewSource(int64(rand.Uint64())))

	for i := 0; i < 100; i++ {
		OneRoundOfMessUpArrowRecords(t, expectedRequest, rng)
	}
}

func OneRoundOfMessUpArrowRecords(t *testing.T, expectedRequest pmetricotlp.ExportRequest, rng *rand.Rand) {
	// Convert the OTLP metrics request to Arrow.
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() {
		pool.AssertSize(t, 0)
	}()

	rBuilder := builder.NewRecordBuilderExt(pool, ametrics.MetricsSchema, DefaultDictConfig, stats.NewProducerStats())
	defer func() {
		rBuilder.Release()
	}()

	var record arrow.Record
	var relatedRecords []*record_message.RecordMessage

	conf := config.DefaultConfig()

	for {
		lb, err := ametrics.NewMetricsBuilder(rBuilder, ametrics.NewConfig(conf), stats.NewProducerStats())
		require.NoError(t, err)
		defer lb.Release()

		err = lb.Append(expectedRequest.Metrics())
		require.NoError(t, err)

		record, err = rBuilder.NewRecord()
		if err == nil {
			relatedRecords, err = lb.RelatedData().BuildRecordMessages()
			require.NoError(t, err)
			break
		}
		require.Error(t, schema.ErrSchemaNotUpToDate)
	}

	// Mix up the Arrow records in such a way as to make decoding impossible.
	mainRecordChanged, record, relatedRecords := MixUpArrowRecords(rng, record, relatedRecords)

	relatedData, _, err := otlp.RelatedDataFrom(relatedRecords)

	// Convert the Arrow records back to OTLP.
	_, err = otlp.MetricsFrom(record, relatedData)

	if mainRecordChanged || relatedData == nil {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	record.Release()
}

func MixUpArrowRecords(rng *rand.Rand, record arrow.Record, relatedRecords []*record_message.RecordMessage) (bool, arrow.Record, []*record_message.RecordMessage) {
	mainRecordChanged := false

	if rng.Intn(100)%2 == 0 {
		// exchange one of the related records with the main record
		relatedRecordPos := rng.Intn(len(relatedRecords))
		relatedRecord := relatedRecords[relatedRecordPos].Record()
		relatedRecords[relatedRecordPos].SetRecord(record)
		record = relatedRecord
		mainRecordChanged = true
	}

	// mix up the related records
	payloadTypes := make([]record_message.PayloadType, len(relatedRecords))
	for i := 0; i < len(relatedRecords); i++ {
		payloadTypes[i] = relatedRecords[i].PayloadType()
	}
	rng.Shuffle(len(payloadTypes), func(i, j int) { payloadTypes[i], payloadTypes[j] = payloadTypes[j], payloadTypes[i] })
	for i := 0; i < len(relatedRecords); i++ {
		relatedRecords[i].SetPayloadType(payloadTypes[i])
	}

	return mainRecordChanged, record, relatedRecords
}
