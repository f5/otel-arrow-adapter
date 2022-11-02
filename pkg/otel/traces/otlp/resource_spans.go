package otlp

import (
	"github.com/apache/arrow/go/v10/arrow"
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrow_utils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/otlp"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

type ResourceSpansIds struct {
	Id         int
	Resource   *otlp.ResourceIds
	SchemaUrl  int
	ScopeSpans *ScopeSpansIds
}

func NewResourceSpansIds(schema *arrow.Schema) (*ResourceSpansIds, error) {
	id, rsDT, err := arrow_utils.ListOfStructsFieldIdFromSchema(schema, constants.RESOURCE_SPANS)
	if err != nil {
		return nil, err
	}

	schemaId, _, err := arrow_utils.FieldIdFromStruct(rsDT, constants.SCHEMA_URL)
	if err != nil {
		return nil, err
	}

	scopeSpansIds, err := NewScopeSpansIds(rsDT)
	if err != nil {
		return nil, err
	}

	resourceIds, err := otlp.NewResourceIds(rsDT)
	if err != nil {
		return nil, err
	}

	return &ResourceSpansIds{
		Id:         id,
		Resource:   resourceIds,
		SchemaUrl:  schemaId,
		ScopeSpans: scopeSpansIds,
	}, nil
}

func AppendResourceSpansInto(traces ptrace.Traces, record arrow.Record, traceIds *TraceIds) error {
	resSpansSlice := traces.ResourceSpans()
	resSpansCount := int(record.NumRows())

	for traceIdx := 0; traceIdx < resSpansCount; traceIdx++ {
		arrowResEnts, err := arrow_utils.ListOfStructsFromRecordBis(record, traceIds.ResourceSpans.Id, traceIdx)
		if err != nil {
			return err
		}
		resSpansSlice.EnsureCapacity(resSpansSlice.Len() + arrowResEnts.End() - arrowResEnts.Start())

		for resSpansIdx := arrowResEnts.Start(); resSpansIdx < arrowResEnts.End(); resSpansIdx++ {
			resSpans := resSpansSlice.AppendEmpty()

			if err = otlp.AppendResourceInto(resSpans, arrowResEnts, resSpansIdx, traceIds.ResourceSpans.Resource); err != nil {
				return err
			}

			schemaUrl, err := arrowResEnts.StringFieldById(traceIds.ResourceSpans.SchemaUrl, resSpansIdx)
			if err != nil {
				return err
			}
			resSpans.SetSchemaUrl(schemaUrl)

			err = AppendScopeSpansInto(resSpans, arrowResEnts, resSpansIdx, traceIds.ResourceSpans.ScopeSpans)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
