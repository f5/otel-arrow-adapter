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

package otlp

import (
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/internal"
	"github.com/f5/otel-arrow-adapter/pkg/otel/metrics/arrow2"
)

func TestHistograms(t *testing.T) {
	t.Parallel()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	s := arrow.NewSchema([]arrow.Field{
		{Name: "histograms", Type: arrow2.UnivariateHistogramDT, Metadata: schema.Metadata(schema.Optional)},
	}, nil)

	rBuilder := builder.NewRecordBuilderExt(pool, s, DefaultDictConfig)
	defer rBuilder.Release()

	var record arrow.Record
	var err error

	maxIter := 10

	// Create Arrow record from OTLP univariate histograms.
	for {
		smdata := &arrow2.ScopeMetricsSharedData{Attributes: &common.SharedAttributes{}}
		mdata := &arrow2.MetricSharedData{Attributes: &common.SharedAttributes{}}
		b := arrow2.UnivariateHistogramBuilderFrom(rBuilder.StructBuilder("histograms"))
		for i := 0; i < maxIter; i++ {
			err = b.Append(internal.Histogram1(), smdata, mdata)
			require.NoError(t, err)
			err = b.Append(internal.Histogram2(), smdata, mdata)
			require.NoError(t, err)
		}

		record, err = rBuilder.NewRecord()
		if err == nil {
			break
		}
		assert.Error(t, schema.ErrSchemaNotUpToDate)
	}
	defer record.Release()

	// Retrieve the Arrow Map representing the histograms.
	arr := record.Columns()[0].(*array.Struct)

	// Check the OTLP Arrow encoding and OTLP decoding by
	// comparing the original and decoded histograms.
	row := 0
	smdata := &SharedData{Attributes: pcommon.NewMap()}
	mdata := &SharedData{Attributes: pcommon.NewMap()}
	histoIds, err := NewUnivariateHistogramIds(arr.DataType().(*arrow.StructType))
	require.NoError(t, err)
	for i := 0; i < maxIter; i++ {

		value := pmetric.NewHistogram()
		err = UpdateUnivariateHistogramFrom(value, arr, row, histoIds, smdata, mdata)
		require.NoError(t, err)
		assert.Equal(t, internal.Histogram1(), value)
		row++

		value = pmetric.NewHistogram()
		err = UpdateUnivariateHistogramFrom(value, arr, row, histoIds, smdata, mdata)
		require.NoError(t, err)
		assert.Equal(t, internal.Histogram2(), value)
		row++
	}
}
