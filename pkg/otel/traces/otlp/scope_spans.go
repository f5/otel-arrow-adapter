package otlp

import (
	"github.com/apache/arrow/go/v10/arrow"
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrow_utils "github.com/f5/otel-arrow-adapter/pkg/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/otlp"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

type ScopeSpansIds struct {
	Id        int
	SchemaUrl int
	ScopeIds  *otlp.ScopeIds
	SpansIds  *SpansIds
}

func NewScopeSpansIds(dt *arrow.StructType) (*ScopeSpansIds, error) {
	id, scopeSpansDT, err := arrow_utils.ListOfStructsFieldIdFromStruct(dt, constants.SCOPE_SPANS)
	if err != nil {
		return nil, err
	}

	schemaId, _, err := arrow_utils.FieldIdFromStruct(scopeSpansDT, constants.SCHEMA_URL)
	if err != nil {
		return nil, err
	}

	scopeIds, err := otlp.NewScopeIds(scopeSpansDT)
	if err != nil {
		return nil, err
	}

	spansIds, err := NewSpansIds(scopeSpansDT)
	if err != nil {
		return nil, err
	}

	return &ScopeSpansIds{
		Id:        id,
		SchemaUrl: schemaId,
		ScopeIds:  scopeIds,
		SpansIds:  spansIds,
	}, nil
}

func AppendScopeSpansInto(resSpans ptrace.ResourceSpans, arrowResSpans *arrow_utils.ListOfStructs, resSpansIdx int, ids *ScopeSpansIds) error {
	arrowScopeSpans, err := arrowResSpans.ListOfStructsById(resSpansIdx, ids.Id)
	if err != nil {
		return err
	}
	scopeSpansSlice := resSpans.ScopeSpans()
	scopeSpansSlice.EnsureCapacity(arrowScopeSpans.End() - arrowResSpans.Start())

	for scopeSpansIdx := arrowScopeSpans.Start(); scopeSpansIdx < arrowScopeSpans.End(); scopeSpansIdx++ {
		scopeSpans := scopeSpansSlice.AppendEmpty()

		if err = otlp.UpdateScopeWith(scopeSpans.Scope(), arrowScopeSpans, scopeSpansIdx, ids.ScopeIds); err != nil {
			return err
		}

		schemaUrl, err := arrowScopeSpans.StringFieldById(ids.SchemaUrl, scopeSpansIdx)
		if err != nil {
			return err
		}
		scopeSpans.SetSchemaUrl(schemaUrl)

		arrowSpans, err := arrowScopeSpans.ListOfStructsById(scopeSpansIdx, ids.SpansIds.Id)
		if err != nil {
			return err
		}
		spansSlice := scopeSpans.Spans()
		spansSlice.EnsureCapacity(arrowSpans.End() - arrowSpans.Start())
		for entityIdx := arrowSpans.Start(); entityIdx < arrowSpans.End(); entityIdx++ {
			err = AppendSpanInto(spansSlice, arrowSpans, entityIdx, ids.SpansIds)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
