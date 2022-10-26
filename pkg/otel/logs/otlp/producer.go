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
	"fmt"

	"github.com/lquerel/otel-arrow-adapter/pkg/air"
	arrow2 "github.com/lquerel/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/common/otlp"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/constants"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/logs"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

type TopLevelWrapper struct {
	logs plog.Logs
}

func (w TopLevelWrapper) ResourceEntities() otlp.ResourceEntitiesSlice[plog.LogRecord] {
	return ResourceLogsSliceWrapper{slice: w.logs.ResourceLogs()}
}

func (w TopLevelWrapper) Unwrap() plog.Logs {
	return w.logs
}

type ResourceLogsSliceWrapper struct {
	slice plog.ResourceLogsSlice
}

func (s ResourceLogsSliceWrapper) EnsureCapacity(newCap int) {
	s.slice.EnsureCapacity(newCap)
}

func (s ResourceLogsSliceWrapper) AppendEmpty() otlp.ResourceEntities[plog.LogRecord] {
	return ResourceLogsWrapper{resourceLogs: s.slice.AppendEmpty()}
}

type ResourceLogsWrapper struct {
	resourceLogs plog.ResourceLogs
}

func (w ResourceLogsWrapper) Resource() pcommon.Resource {
	return w.resourceLogs.Resource()
}
func (w ResourceLogsWrapper) SetSchemaUrl(schemaUrl string) {
	w.resourceLogs.SetSchemaUrl(schemaUrl)
}
func (w ResourceLogsWrapper) ScopeEntities() otlp.ScopeEntities[plog.LogRecord] {
	return ScopeLogsWrapper{scopeLogs: w.resourceLogs.ScopeLogs().AppendEmpty()}
}

type ScopeLogsWrapper struct {
	scopeLogs plog.ScopeLogs
}

func (w ScopeLogsWrapper) Scope() pcommon.InstrumentationScope {
	return w.scopeLogs.Scope()
}
func (w ScopeLogsWrapper) SetSchemaUrl(schemaUrl string) {
	w.scopeLogs.SetSchemaUrl(schemaUrl)
}
func (w ScopeLogsWrapper) Entity() plog.LogRecord {
	return w.scopeLogs.LogRecords().AppendEmpty()
}

type LogsProducer struct {
	logs.Constants
}

func (p LogsProducer) NewTopLevelEntities() otlp.TopLevelEntities[plog.Logs, plog.LogRecord] {
	return TopLevelWrapper{plog.NewLogs()}
}
func (p LogsProducer) EntityProducer(scopeLog otlp.ScopeEntities[plog.LogRecord], los *air.ListOfStructs, row int) error {
	log := scopeLog.Entity()
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
		if err := arrow2.CopyValueFrom(log.Body(), bodyArray.DataType(), bodyArray, row); err != nil {
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
