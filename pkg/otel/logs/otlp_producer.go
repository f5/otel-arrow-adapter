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

package logs

import (
	"fmt"

	"github.com/apache/arrow/go/v9/arrow"

	"github.com/lquerel/otel-arrow-adapter/pkg/air"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/common"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/constants"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// OtlpProducer produces OTLP logs from OTLP Arrow logs.
type OtlpProducer struct {
}

// NewOtlpProducer creates a new OtlpProducer.
func NewOtlpProducer() *OtlpProducer {
	return &OtlpProducer{}
}

// ProduceFrom produces OTLP logs from an Arrow Record.
func (p *OtlpProducer) ProduceFrom(record arrow.Record) ([]plog.Logs, error) {
	return ArrowRecordToOtlpLogs(record)
}

// ArrowRecordToOtlpLogs converts an Arrow Record to an OTLP Logs.
// TODO: add a reference to the OTEP 0156 section that describes this mapping.
func ArrowRecordToOtlpLogs(record arrow.Record) ([]plog.Logs, error) {
	// Each first level row in the Arrow Record represents a resource log entity.
	resLogCount := int(record.NumRows())
	allLogs := make([]plog.Logs, 0, resLogCount)

	for logIdx := 0; logIdx < resLogCount; logIdx++ {
		logs := plog.NewLogs()

		arrowResLogs, err := air.ListOfStructsFromRecord(record, constants.RESOURCE_LOGS, logIdx)
		if err != nil {
			return allLogs, err
		}
		logs.ResourceLogs().EnsureCapacity(arrowResLogs.End() - arrowResLogs.Start())

		for resLogIdx := arrowResLogs.Start(); resLogIdx < arrowResLogs.End(); resLogIdx++ {
			resLog := logs.ResourceLogs().AppendEmpty()

			resource, err := common.NewResourceFrom(arrowResLogs, resLogIdx)
			if err != nil {
				return allLogs, err
			}
			resource.CopyTo(resLog.Resource())

			schemaUrl, err := arrowResLogs.StringFieldByName(constants.SCHEMA_URL, resLogIdx)
			if err != nil {
				return allLogs, err
			}
			resLog.SetSchemaUrl(schemaUrl)

			arrowScopeLogs, err := arrowResLogs.ListOfStructsByName(constants.SCOPE_LOGS, resLogIdx)
			if err != nil {
				return allLogs, err
			}
			for scopeLogIdx := arrowScopeLogs.Start(); scopeLogIdx < arrowScopeLogs.End(); scopeLogIdx++ {
				scopeLog := resLog.ScopeLogs().AppendEmpty()

				scope, err := common.NewScopeFrom(arrowScopeLogs, scopeLogIdx)
				if err != nil {
					return allLogs, err
				}
				scope.CopyTo(scopeLog.Scope())

				schemaUrl, err := arrowScopeLogs.StringFieldByName(constants.SCHEMA_URL, scopeLogIdx)
				if err != nil {
					return allLogs, err
				}
				scopeLog.SetSchemaUrl(schemaUrl)

				arrowLogs, err := arrowScopeLogs.ListOfStructsByName(constants.LOGS, scopeLogIdx)
				if err != nil {
					return allLogs, err
				}
				for logIdx := arrowLogs.Start(); logIdx < arrowLogs.End(); logIdx++ {
					logRecord := scopeLog.LogRecords().AppendEmpty()
					err = SetLogFrom(logRecord, arrowLogs, logIdx)
					if err != nil {
						return allLogs, err
					}
				}
			}
		}

		allLogs = append(allLogs, logs)
	}

	return allLogs, nil
}

// SetLogFrom initializes a LogRecord from an Arrow representation.
func SetLogFrom(log plog.LogRecord, los *air.ListOfStructs, row int) error {
	timeUnixNano, err := los.U64FieldByName(constants.TIME_UNIX_NANO, row)
	if err != nil {
		return err
	}
	observedTimeUnixNano, err := los.U64FieldByName(constants.OBSERVED_TIME_UNIX_NANO, row)
	if err != nil {
		return err
	}
	traceId, err := los.BinaryFieldByName(constants.TRACE_ID, row)
	if err != nil {
		return err
	}
	if len(traceId) != 16 {
		return fmt.Errorf("trace_id field should be 16 bytes")
	}
	spanId, err := los.BinaryFieldByName(constants.SPAN_ID, row)
	if err != nil {
		return err
	}
	severityNumber, err := los.I32FieldByName(constants.SEVERITY_NUMBER, row)
	if err != nil {
		return err
	}
	severityText, err := los.StringFieldByName(constants.SEVERITY_TEXT, row)
	if err != nil {
		return err
	}
	bodyArray, ok := los.Field(constants.BODY)
	if ok {
		if err := common.CopyValueFrom(log.Body(), bodyArray.DataType(), bodyArray, row); err != nil {
			return err
		}
	}
	droppedAttributesCount, err := los.U32FieldByName(constants.DROPPED_ATTRIBUTES_COUNT, row)
	if err != nil {
		return err
	}
	attrs, err := los.ListOfStructsByName(constants.ATTRIBUTES, row)
	if err != nil {
		return err
	}
	if attrs != nil {
		err = attrs.CopyAttributesFrom(log.Attributes())
	}
	flags, err := los.U32FieldByName(constants.FLAGS, row)
	if err != nil {
		return err
	}

	var tid pcommon.TraceID
	var sid pcommon.SpanID
	copy(tid[:], traceId)
	copy(sid[:], spanId)

	log.SetTimestamp(pcommon.Timestamp(timeUnixNano))
	log.SetObservedTimestamp(pcommon.Timestamp(observedTimeUnixNano))
	log.SetTraceID(tid)
	log.SetSpanID(sid)
	log.SetSeverityNumber(plog.SeverityNumber(severityNumber))
	log.SetSeverityText(severityText)
	log.SetFlags(plog.LogRecordFlags(flags))
	log.SetDroppedAttributesCount(droppedAttributesCount)

	return nil
}
