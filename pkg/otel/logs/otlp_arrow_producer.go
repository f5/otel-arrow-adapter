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
	"strings"

	"github.com/apache/arrow/go/v9/arrow"

	"github.com/lquerel/otel-arrow-adapter/pkg/air"
	"github.com/lquerel/otel-arrow-adapter/pkg/air/config"
	"github.com/lquerel/otel-arrow-adapter/pkg/air/rfield"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/common"
	"github.com/lquerel/otel-arrow-adapter/pkg/otel/constants"

	"go.opentelemetry.io/collector/pdata/plog"
)

// OtlpArrowProducer produces OTLP Arrow records from OTLP logs.
type OtlpArrowProducer struct {
	cfg *config.Config
	rr  *air.RecordRepository
}

// NewOtlpArrowProducer creates a new OtlpArrowProducer with the default configuration.
// Note: the default attribute encoding is AttributesAsListStructs
func NewOtlpArrowProducer() *OtlpArrowProducer {
	cfg := config.NewUint16DefaultConfig()
	cfg.Attribute.Encoding = config.AttributesAsListStructs

	return &OtlpArrowProducer{
		cfg: cfg,
		rr:  air.NewRecordRepository(cfg),
	}
}

// NewOtlpArrowProducerWith creates a new OtlpArrowProducer with the given configuration.
func NewOtlpArrowProducerWith(cfg *config.Config) *OtlpArrowProducer {
	return &OtlpArrowProducer{
		cfg: cfg,
		rr:  air.NewRecordRepository(cfg),
	}
}

// ProduceFrom produces Arrow records from the given OTLP logs. The generated schemas of the Arrow records follow
// the hierarchical organization of the log protobuf structure.
//
// Resource signature = resource attributes sig + dropped attributes count sig + schema URL sig
//
// More details can be found in the OTEL 0156 section XYZ.
// TODO: add a reference to the OTEP 0156 section that describes this mapping.
func (p *OtlpArrowProducer) ProduceFrom(logs plog.Logs) ([]arrow.Record, error) {
	resLogList := logs.ResourceLogs()
	// Resource logs grouped per signature. The resource log signature is based on the resource attributes, the dropped
	// attributes count, the schema URL, and the scope logs signature.
	resLogsPerSig := make(map[string]*common.ResourceEntity)

	for rsIdx := 0; rsIdx < resLogList.Len(); rsIdx++ {
		resLogs := resLogList.At(rsIdx)

		// Add resource fields (attributes and dropped attributes count)
		resField, resSig := common.ResourceFieldWithSig(resLogs.Resource(), p.cfg)

		// Add schema URL
		var schemaUrl *rfield.Field
		if resLogs.SchemaUrl() != "" {
			schemaUrl = rfield.NewStringField(constants.SCHEMA_URL, resLogs.SchemaUrl())
			resSig += ",schema_url:" + resLogs.SchemaUrl()
		}

		// Group logs per scope span signature
		logsPerScopeLogSig := GroupScopeLogs(resLogs, p.cfg)

		// Create a new entry in the map if the signature is not already present
		resLogFields := resLogsPerSig[resSig]
		if resLogFields == nil {
			resLogFields = common.NewResourceEntity(resField, schemaUrl)
			resLogsPerSig[resSig] = resLogFields
		}

		// Merge logs sharing the same scope log signature
		for sig, sl := range logsPerScopeLogSig {
			scopeLog := resLogFields.ScopeEntities[sig]
			if scopeLog == nil {
				resLogFields.ScopeEntities[sig] = sl
			} else {
				scopeLog.Entities = append(scopeLog.Entities, sl.Entities...)
			}
		}
	}

	// All resource logs sharing the same signature are represented as an AIR record.
	for _, resLogFields := range resLogsPerSig {
		record := air.NewRecord()
		record.ListField(constants.RESOURCE_LOGS, rfield.List{Values: []rfield.Value{
			resLogFields.AirValue(constants.SCOPE_LOGS, constants.LOGS),
		}})
		p.rr.AddRecord(record)
	}

	// Build all Arrow records from the AIR records
	records, err := p.rr.BuildRecords()
	if err != nil {
		return nil, err
	}

	return records, nil
}

// GroupScopeLogs groups logs per signature scope logs signature.
// A scope log signature is based on the combination of scope attributes, dropped attributes count, the schema URL, and
// log signatures.
func GroupScopeLogs(resourceSpans plog.ResourceLogs, cfg *config.Config) (scopeLogsPerSig map[string]*common.EntityGroup) {
	scopeLogList := resourceSpans.ScopeLogs()
	scopeLogsPerSig = make(map[string]*common.EntityGroup, scopeLogList.Len())

	for j := 0; j < scopeLogList.Len(); j++ {
		scopeLogs := scopeLogList.At(j)

		var sig strings.Builder

		scopeField := common.ScopeField(constants.SCOPE, scopeLogs.Scope(), cfg)
		scopeField.Normalize()
		scopeField.WriteSigType(&sig)

		var schemaField *rfield.Field
		if scopeLogs.SchemaUrl() != "" {
			schemaField = rfield.NewStringField(constants.SCHEMA_URL, scopeLogs.SchemaUrl())
			sig.WriteString(",")
			schemaField.WriteSig(&sig)
		}

		// Group logs per signature
		logs := groupLogs(scopeLogs, cfg)

		for logSig, logGroup := range logs {
			sig.WriteByte(',')
			sig.WriteString(logSig)

			// Create a new entry in the map if the signature is not already present
			ssSig := sig.String()
			ssFields := scopeLogsPerSig[ssSig]
			if ssFields == nil {
				ssFields = &common.EntityGroup{
					Scope:     scopeField,
					Entities:  make([]rfield.Value, 0, 16),
					SchemaUrl: schemaField,
				}
				scopeLogsPerSig[ssSig] = ssFields
			}

			ssFields.Entities = append(ssFields.Entities, logGroup...)
		}
	}
	return
}

// groupLogs converts plog.ScopeLogs into their AIR representation and groups them based on a given configuration.
// A log signature is based on the log attributes when the attribute encoding configuration is
// AttributesAsStructs, otherwise it is an empty string.
func groupLogs(scopeLogs plog.ScopeLogs, cfg *config.Config) (logsPerSig map[string][]rfield.Value) {
	logRecordList := scopeLogs.LogRecords()
	logsPerSig = make(map[string][]rfield.Value, logRecordList.Len())

	for k := 0; k < logRecordList.Len(); k++ {
		var logSig strings.Builder
		log := logRecordList.At(k)

		fields := make([]*rfield.Field, 0, 10)
		if ts := log.Timestamp(); ts > 0 {
			fields = append(fields, rfield.NewU64Field(constants.TIME_UNIX_NANO, uint64(ts)))
		}
		if ts := log.ObservedTimestamp(); ts > 0 {
			fields = append(fields, rfield.NewU64Field(constants.OBSERVED_TIME_UNIX_NANO, uint64(ts)))
		}
		if tid := log.TraceID(); !tid.IsEmpty() {
			fields = append(fields, rfield.NewBinaryField(constants.TRACE_ID, tid[:]))
		}
		if sid := log.SpanID(); !sid.IsEmpty() {
			fields = append(fields, rfield.NewBinaryField(constants.SPAN_ID, sid[:]))
		}
		fields = append(fields, rfield.NewI32Field(constants.SEVERITY_NUMBER, int32(log.SeverityNumber())))
		if log.SeverityText() != "" {
			fields = append(fields, rfield.NewStringField(constants.SEVERITY_TEXT, log.SeverityText()))
		}
		body := common.OtlpAnyValueToValue(log.Body())
		if body != nil {
			fields = append(fields, rfield.NewField(constants.BODY, body))
		}

		attributes := common.NewAttributes(log.Attributes(), cfg)
		if attributes != nil {
			fields = append(fields, attributes)
			if cfg.Attribute.Encoding == config.AttributesAsStructs {
				attributes.Normalize()
				attributes.WriteSigType(&logSig)
			}
		}

		if dc := log.DroppedAttributesCount(); dc > 0 {
			fields = append(fields, rfield.NewU32Field(constants.DROPPED_ATTRIBUTES_COUNT, uint32(dc)))
		}

		if log.Flags() > 0 {
			fields = append(fields, rfield.NewU32Field(constants.FLAGS, uint32(log.Flags())))
		}

		logValue := rfield.NewStruct(fields)

		lsig := logSig.String()
		logsPerSig[lsig] = append(logsPerSig[lsig], logValue)
	}
	return
}

// OtlpLogsToArrowRecords converts an OTLP ResourceLogs to one or more Arrow records
func OtlpLogsToArrowRecords(rr *air.RecordRepository, request plog.Logs, cfg *config.Config) ([]arrow.Record, error) {
	for i := 0; i < request.ResourceLogs().Len(); i++ {
		resourceLogs := request.ResourceLogs().At(i)

		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			scopeLogs := resourceLogs.ScopeLogs().At(j)

			for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
				record := air.NewRecord()
				log := scopeLogs.LogRecords().At(k)

				if ts := log.Timestamp(); ts > 0 {
					record.U64Field(constants.TIME_UNIX_NANO, uint64(ts))
				}
				if ots := log.ObservedTimestamp(); ots > 0 {
					record.U64Field(constants.OBSERVED_TIME_UNIX_NANO, uint64(ots))
				}
				common.AddResource(record, resourceLogs.Resource(), cfg)
				common.AddScope(record, constants.SCOPE_LOGS, scopeLogs.Scope(), cfg)

				record.I32Field(constants.SEVERITY_NUMBER, int32(log.SeverityNumber()))
				if log.SeverityText() != "" {
					record.StringField(constants.SEVERITY_TEXT, log.SeverityText())
				}
				body := common.OtlpAnyValueToValue(log.Body())
				if body != nil {
					record.GenericField(constants.BODY, body)
				}
				attributes := common.NewAttributes(log.Attributes(), cfg)
				if attributes != nil {
					record.AddField(attributes)
				}

				if dc := log.DroppedAttributesCount(); dc > 0 {
					record.U32Field(constants.DROPPED_ATTRIBUTES_COUNT, uint32(dc))
				}
				if log.Flags() > 0 {
					record.U32Field(constants.FLAGS, uint32(log.Flags()))
				}
				if tid := log.TraceID(); !tid.IsEmpty() {
					record.BinaryField(constants.TRACE_ID, tid[:])
				}
				if sid := log.SpanID(); !sid.IsEmpty() {
					record.BinaryField(constants.SPAN_ID, sid[:])
				}

				rr.AddRecord(record)
			}
		}
	}

	logsRecords, err := rr.BuildRecords()
	if err != nil {
		return nil, err
	}

	result := make([]arrow.Record, 0, len(logsRecords))
	for _, record := range logsRecords {
		result = append(result, record)
	}

	return result, nil
}
