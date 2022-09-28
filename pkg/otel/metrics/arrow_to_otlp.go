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

package metrics

import (
	"fmt"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"

	"otel-arrow-adapter/pkg/air"
	"otel-arrow-adapter/pkg/otel/common"
	"otel-arrow-adapter/pkg/otel/constants"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func ArrowRecordsToOtlpMetrics(record arrow.Record) (pmetric.Metrics, error) {
	request := pmetric.NewMetrics()

	resourceMetrics := map[string]pmetric.ResourceMetrics{}
	scopeMetrics := map[string]pmetric.ScopeMetrics{}

	numRows := int(record.NumRows())
	for i := 0; i < numRows; i++ {
		resource, err := common.NewResourceFrom(record, i)
		if err != nil {
			return request, err
		}
		resId := common.ResourceId(resource)
		rm, ok := resourceMetrics[resId]
		if !ok {
			rm = request.ResourceMetrics().AppendEmpty()
			// TODO: SchemaURL
			resource.CopyTo(rm.Resource())
			resourceMetrics[resId] = rm
		}

		scope, err := common.NewInstrumentationScopeFrom(record, i, constants.SCOPE_METRICS)
		if err != nil {
			return request, err
		}
		scopeSpanId := resId + "|" + common.ScopeId(scope)
		sm, ok := scopeMetrics[scopeSpanId]
		if !ok {
			sm = rm.ScopeMetrics().AppendEmpty()
			scope.CopyTo(sm.Scope())
			// TODO: SchemaURL
			scopeMetrics[scopeSpanId] = sm
		}
		if err := SetMetricsFrom(sm.Metrics(), record, i); err != nil {
			return request, err
		}
	}

	return request, nil
}

func SetMetricsFrom(metrics pmetric.MetricSlice, record arrow.Record, row int) error {
	timeUnixNano, err := air.U64FromRecord(record, row, constants.TIME_UNIX_NANO)
	if err != nil {
		return err
	}
	startTimeUnixNano, err := air.U64FromRecord(record, row, constants.START_TIME_UNIX_NANO)
	if err != nil {
		return err
	}
	flags, err := air.U32FromRecord(record, row, constants.FLAGS)
	if err != nil {
		return err
	}
	metricsField, arr := air.FieldArray(record, constants.METRICS)
	if metricsField == nil {
		return fmt.Errorf("no metrics found")
	}
	metricsType, ok := metricsField.Type.(*arrow.StructType)
	if !ok {
		return fmt.Errorf("metrics type is not a struct")
	}
	metricsArr, ok := arr.(*array.Struct)
	if !ok {
		return fmt.Errorf("metrics array is not a struct")
	}

	attrsField, attrsArray := air.FieldArray(record, constants.ATTRIBUTES)
	attributes := pcommon.NewMap()
	if attrsField != nil {
		if err := common.CopyAttributesFrom(attributes, attrsField.Type, attrsArray, row); err != nil {
			return err
		}
	}
	for i, metricField := range metricsType.Fields() {
		metricType := metricMetadata(&metricField, constants.METADATA_METRIC_TYPE)
		metricArr := metricsArr.Field(i)

		switch metricType {
		case constants.SUM_METRICS:
			err := collectSumMetrics(metrics, timeUnixNano, startTimeUnixNano, flags, metricField, metricArr, row, attributes)
			if err != nil {
				return err
			}
		case constants.GAUGE_METRICS:
			err := collectGaugeMetrics(metrics, timeUnixNano, startTimeUnixNano, flags, metricField, metricArr, row, attributes)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported metric type: %s", metricType)
		}
	}

	return nil
}

func collectSumMetrics(metrics pmetric.MetricSlice, timeUnixNano uint64, startTimeUnixNano uint64, flags uint32, metricField arrow.Field, metricArr arrow.Array, row int, attributes pcommon.Map) error {
	metricName := metricField.Name
	switch dt := metricField.Type.(type) {
	case *arrow.Int64Type:
		m := metrics.AppendEmpty()
		m.SetName(metricName)
		m.SetDescription(metricMetadata(&metricField, constants.METADATA_METRIC_DESCRIPTION))
		m.SetUnit(metricMetadata(&metricField, constants.METADATA_METRIC_UNIT))
		sum := m.SetEmptySum()
		// TODO: Add isMonotonic
		// TODO: Add temporality
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.MetricAggregationTemporalityCumulative)

		dp, err := collectI64NumberDataPoint(sum.DataPoints(), timeUnixNano, startTimeUnixNano, flags, metricArr, row, attributes)
		if err != nil {
			return err
		}

		return []*pmetric.Metric{{
			Data: &pmetric.Metric_Sum{
				Sum: &pmetric.Sum{
					DataPoints:             []*pmetric.NumberDataPoint{dp},
					AggregationTemporality: 0,     // ToDo Add aggregation temporality
					IsMonotonic:            false, // ToDo Add is monotonic
				},
			},
		}}, nil
	case *arrow.Float64Type:
		dp, err := collectF64NumberDataPoint(timeUnixNano, startTimeUnixNano, flags, metricArr, row, attributes)
		if err != nil {
			return nil, err
		}
		return []*pmetric.Metric{{
			Name:        metricName,
			Description: metricMetadata(&metricField, constants.METADATA_METRIC_DESCRIPTION),
			Unit:        metricMetadata(&metricField, constants.METADATA_METRIC_UNIT),
			Data: &pmetric.Metric_Sum{
				Sum: &pmetric.Sum{
					DataPoints:             []*pmetric.NumberDataPoint{dp},
					AggregationTemporality: 0,     // ToDo Add aggregation temporality
					IsMonotonic:            false, // ToDo Add is monotonic
				},
			},
		}}, nil
	case *arrow.StructType:
		mm, err := collectMultivariateSumMetrics(timeUnixNano, startTimeUnixNano, flags, &metricField, metricArr, metricName, row, attributes)
		if err != nil {
			return nil, err
		}
		return mm, nil
	default:
		return nil, fmt.Errorf("unsupported metric type: %T", dt)
	}
}

func collectGaugeMetrics(metrics pmetric.MetricSlice, timeUnixNano uint64, startTimeUnixNano uint64, flags uint32, metricField arrow.Field, metricArr arrow.Array, row int, attributes []*pcommon.KeyValue) error {
	metricName := metricField.Name
	switch dt := metricField.Type.(type) {
	case *arrow.Int64Type:
		dp, err := collectI64NumberDataPoint(metrics, timeUnixNano, startTimeUnixNano, flags, metricArr, row, attributes)
		if err != nil {
			return err
		}
		return []*pmetric.Metric{{
			Name:        metricName,
			Description: metricMetadata(&metricField, constants.METADATA_METRIC_DESCRIPTION),
			Unit:        metricMetadata(&metricField, constants.METADATA_METRIC_UNIT),
			Data: &pmetric.Metric_Gauge{
				Gauge: &pmetric.Gauge{
					DataPoints: []*pmetric.NumberDataPoint{dp},
				},
			},
		}}, nil
	case *arrow.Float64Type:
		dp, err := collectF64NumberDataPoint(timeUnixNano, startTimeUnixNano, flags, metricArr, row, attributes)
		if err != nil {
			return nil, err
		}
		return []*pmetric.Metric{{
			Name:        metricName,
			Description: metricMetadata(&metricField, constants.METADATA_METRIC_DESCRIPTION),
			Unit:        metricMetadata(&metricField, constants.METADATA_METRIC_UNIT),
			Data: &pmetric.Metric_Gauge{
				Gauge: &pmetric.Gauge{
					DataPoints: []*pmetric.NumberDataPoint{dp},
				},
			},
		}}, nil
	case *arrow.StructType:
		mm, err := collectMultivariateGaugeMetrics(timeUnixNano, startTimeUnixNano, flags, &metricField, metricArr, metricName, row, attributes)
		if err != nil {
			return nil, err
		}
		return mm, nil
	default:
		return nil, fmt.Errorf("unsupported metric type: %T", dt)
	}
}

func collectI64NumberDataPoint(timeUnixNano uint64, startTimeUnixNano uint64, flags uint32, metricArr arrow.Array, row int, attributes []*pcommon.KeyValue) (*pmetric.NumberDataPoint, error) {
	v, err := air.I64FromArray(metricArr, row)
	if err != nil {
		return nil, err
	}
	return &pmetric.NumberDataPoint{
		Attributes:        attributes,
		StartTimeUnixNano: startTimeUnixNano,
		TimeUnixNano:      timeUnixNano,
		Value: &pmetric.NumberDataPoint_AsInt{
			AsInt: v,
		},
		Exemplars: nil, // ToDo Add exemplars
		Flags:     flags,
	}, nil
}

func collectF64NumberDataPoint(timeUnixNano uint64, startTimeUnixNano uint64, flags uint32, metricArr arrow.Array, row int, attributes []*pcommon.KeyValue) (*pmetric.NumberDataPoint, error) {
	v, err := air.F64FromArray(metricArr, row)
	if err != nil {
		return nil, err
	}
	return &pmetric.NumberDataPoint{
		Attributes:        attributes,
		StartTimeUnixNano: startTimeUnixNano,
		TimeUnixNano:      timeUnixNano,
		Value: &pmetric.NumberDataPoint_AsDouble{
			AsDouble: v,
		},
		Exemplars: nil, // ToDo Add exemplars
		Flags:     flags,
	}, nil
}

func collectMultivariateSumMetrics(timeUnixNano uint64, startTimeUnixNano uint64, flags uint32, field *arrow.Field, arr arrow.Array, name string, row int, attributes []*pcommon.KeyValue) ([]*pmetric.Metric, error) {
	metricFields := field.Type.(*arrow.StructType).Fields()
	multivariateArr, ok := arr.(*array.Struct)
	if !ok {
		return nil, fmt.Errorf("metrics array is not a struct")
	}

	dataPoints := make([]*pmetric.NumberDataPoint, 0, len(metricFields))
	for i, metricField := range metricFields {
		metricArr := multivariateArr.Field(i)

		extAttributes := make([]*pcommon.KeyValue, len(attributes)+1)
		copy(extAttributes, attributes)
		extAttributes[len(attributes)] = &pcommon.KeyValue{
			Key: metricMetadata(&metricField, constants.METADATA_METRIC_MULTIVARIATE_ATTR),
			Value: &pcommon.AnyValue{
				Value: &pcommon.AnyValue_StringValue{
					StringValue: metricField.Name,
				},
			},
		}

		switch dt := metricField.Type.(type) {
		case *arrow.Int64Type:
			dp, err := collectI64NumberDataPoint(timeUnixNano, startTimeUnixNano, flags, metricArr, row, extAttributes)
			if err != nil {
				return nil, err
			}
			dataPoints = append(dataPoints, dp)
		case *arrow.Float64Type:
			dp, err := collectF64NumberDataPoint(timeUnixNano, startTimeUnixNano, flags, metricArr, row, extAttributes)
			if err != nil {
				return nil, err
			}
			dataPoints = append(dataPoints, dp)
		default:
			return nil, fmt.Errorf("unsupported metric type: %T", dt)
		}
	}

	return []*pmetric.Metric{
		{
			Name:        name,
			Description: metricMetadata(field, constants.METADATA_METRIC_DESCRIPTION),
			Unit:        metricMetadata(field, constants.METADATA_METRIC_UNIT),
			Data: &pmetric.Metric_Sum{
				Sum: &pmetric.Sum{
					DataPoints:             dataPoints,
					AggregationTemporality: 0,     // ToDo Add aggregation temporality
					IsMonotonic:            false, // ToDo Add is monotonic
				},
			},
		},
	}, nil
}

func collectMultivariateGaugeMetrics(timeUnixNano uint64, startTimeUnixNano uint64, flags uint32, field *arrow.Field, arr arrow.Array, name string, row int, attributes []*pcommon.KeyValue) ([]*pmetric.Metric, error) {
	metricFields := field.Type.(*arrow.StructType).Fields()
	multivariateArr, ok := arr.(*array.Struct)
	if !ok {
		return nil, fmt.Errorf("metrics array is not a struct")
	}

	dataPoints := make([]*pmetric.NumberDataPoint, 0, len(metricFields))
	for i, metricField := range metricFields {
		metricArr := multivariateArr.Field(i)

		extAttributes := make([]*pcommon.KeyValue, len(attributes)+1)
		copy(extAttributes, attributes)
		extAttributes[len(attributes)] = &pcommon.KeyValue{
			Key: metricMetadata(&metricField, constants.METADATA_METRIC_MULTIVARIATE_ATTR),
			Value: &pcommon.AnyValue{
				Value: &pcommon.AnyValue_StringValue{
					StringValue: metricField.Name,
				},
			},
		}

		switch dt := metricField.Type.(type) {
		case *arrow.Int64Type:
			dp, err := collectI64NumberDataPoint(timeUnixNano, startTimeUnixNano, flags, metricArr, row, extAttributes)
			if err != nil {
				return nil, err
			}
			dataPoints = append(dataPoints, dp)
		case *arrow.Float64Type:
			dp, err := collectF64NumberDataPoint(timeUnixNano, startTimeUnixNano, flags, metricArr, row, extAttributes)
			if err != nil {
				return nil, err
			}
			dataPoints = append(dataPoints, dp)
		default:
			return nil, fmt.Errorf("unsupported metric type: %T", dt)
		}
	}

	return []*pmetric.Metric{
		{
			Name:        name,
			Description: metricMetadata(field, constants.METADATA_METRIC_DESCRIPTION),
			Unit:        metricMetadata(field, constants.METADATA_METRIC_UNIT),
			Data: &pmetric.Metric_Sum{
				Sum: &pmetric.Sum{
					DataPoints:             dataPoints,
					AggregationTemporality: 0,     // ToDo Add aggregation temporality
					IsMonotonic:            false, // ToDo Add is monotonic
				},
			},
		},
	}, nil
}

func metricMetadata(field *arrow.Field, metadata string) string {
	if field.HasMetadata() {
		idx := field.Metadata.FindKey(metadata)
		if idx != -1 {
			return field.Metadata.Values()[idx]
		} else {
			return ""
		}
	} else {
		return ""
	}
}
