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

package otlp

import (
	"github.com/apache/arrow/go/v12/arrow"
	"go.opentelemetry.io/collector/pdata/pmetric"

	arrowutils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/otlp"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
	"github.com/f5/otel-arrow-adapter/pkg/werror"
)

type ScopeMetricsIds struct {
	ID        int
	SchemaUrl int
	ScopeIDs  *otlp.ScopeIds
}

func NewScopeMetricsIds(scopeMetricsDT *arrow.StructType) (*ScopeMetricsIds, error) {
	ID, _ := arrowutils.FieldIDFromStruct(scopeMetricsDT, constants.ID)

	scopeIds, err := otlp.NewScopeIds(scopeMetricsDT)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	schemaID, _ := arrowutils.FieldIDFromStruct(scopeMetricsDT, constants.SchemaUrl)

	return &ScopeMetricsIds{
		ID:        ID,
		SchemaUrl: schemaID,
		ScopeIDs:  scopeIds,
	}, nil
}

func UpdateScopeMetricsFrom(
	scopeMetricsSlice pmetric.ScopeMetricsSlice,
	arrowScopeMetrics *arrowutils.ListOfStructs,
	ids *ScopeMetricsIds,
	relatedData *RelatedData,
) error {
	scopeMetricsSlice.EnsureCapacity(arrowScopeMetrics.End() - arrowScopeMetrics.Start())

	for scopeMetricsIdx := arrowScopeMetrics.Start(); scopeMetricsIdx < arrowScopeMetrics.End(); scopeMetricsIdx++ {
		scopeMetrics := scopeMetricsSlice.AppendEmpty()

		if err := otlp.UpdateScopeWith(scopeMetrics.Scope(), arrowScopeMetrics, scopeMetricsIdx, ids.ScopeIDs, relatedData.ScopeAttrMapStore); err != nil {
			return werror.Wrap(err)
		}

		ID, err := arrowScopeMetrics.U16FieldByID(ids.ID, scopeMetricsIdx)
		schemaUrl, err := arrowScopeMetrics.StringFieldByID(ids.SchemaUrl, scopeMetricsIdx)
		if err != nil {
			return werror.Wrap(err)
		}
		scopeMetrics.SetSchemaUrl(schemaUrl)

		numberDPs := relatedData.NumberDataPointsStore.NumberDataPointsByID(ID)
		summaries := relatedData.SummaryDataPointsStore.SummaryMetricsByID(ID)
		histograms := relatedData.HistogramDataPointsStore.HistogramMetricsByID(ID)
		eHistograms := relatedData.EHistogramDataPointsStore.EHistogramMetricsByID(ID)

		metrics := scopeMetrics.Metrics()
		metrics.EnsureCapacity(len(numberDPs) + len(summaries) + len(histograms) + len(eHistograms))
		for _, sum := range numberDPs {
			sum.MoveTo(metrics.AppendEmpty())
		}
		for _, sum := range summaries {
			sum.MoveTo(metrics.AppendEmpty())
		}
		for _, hist := range histograms {
			hist.MoveTo(metrics.AppendEmpty())
		}
		for _, hist := range eHistograms {
			hist.MoveTo(metrics.AppendEmpty())
		}
	}

	return nil
}
