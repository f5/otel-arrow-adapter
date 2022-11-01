package otlp

import (
	"github.com/apache/arrow/go/v10/arrow"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/f5/otel-arrow-adapter/pkg/air"
	common_arrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

func TracesFrom(record arrow.Record) (ptrace.Traces, error) {
	traces := ptrace.NewTraces()
	resSpansSlice := traces.ResourceSpans()
	resSpansCount := int(record.NumRows())
	resSpansSlice.EnsureCapacity(resSpansCount)

	for traceIdx := 0; traceIdx < resSpansCount; traceIdx++ {
		arrowResEnts, err := air.ListOfStructsFromRecord(record, constants.RESOURCE_SPANS, traceIdx)
		if err != nil {
			return traces, err
		}
		resSpansSlice.EnsureCapacity(resSpansSlice.Len() + arrowResEnts.End() - arrowResEnts.Start())

		for resSpansIdx := arrowResEnts.Start(); resSpansIdx < arrowResEnts.End(); resSpansIdx++ {
			resSpans := resSpansSlice.AppendEmpty()

			resource, err := common_arrow.NewResourceFrom(arrowResEnts, resSpansIdx)
			if err != nil {
				return traces, err
			}
			resource.CopyTo(resSpans.Resource())

			schemaUrl, err := arrowResEnts.StringFieldByName(constants.SCHEMA_URL, resSpansIdx)
			if err != nil {
				return traces, err
			}
			resSpans.SetSchemaUrl(schemaUrl)

			arrowScopeSpans, err := arrowResEnts.ListOfStructsByName(constants.SCOPE_SPANS, resSpansIdx)
			if err != nil {
				return traces, err
			}
			scopeSpansSlice := resSpans.ScopeSpans()
			for scopeSpansIdx := arrowScopeSpans.Start(); scopeSpansIdx < arrowScopeSpans.End(); scopeSpansIdx++ {
				scopeSpans := scopeSpansSlice.AppendEmpty()

				scope, err := common_arrow.NewScopeFrom(arrowScopeSpans, scopeSpansIdx)
				if err != nil {
					return traces, err
				}
				scope.CopyTo(scopeSpans.Scope())

				schemaUrl, err := arrowScopeSpans.StringFieldByName(constants.SCHEMA_URL, scopeSpansIdx)
				if err != nil {
					return traces, err
				}
				scopeSpans.SetSchemaUrl(schemaUrl)

				arrowSpans, err := arrowScopeSpans.ListOfStructsByName(constants.SPANS, scopeSpansIdx)
				if err != nil {
					return traces, err
				}
				for entityIdx := arrowSpans.Start(); entityIdx < arrowSpans.End(); entityIdx++ {
					// TODO - implement
					//err = p.entitiesProducer.EntityProducer(scopeSpans, arrowSpans, entityIdx)
					//if err != nil {
					//	return traces, err
					//}
				}
			}
		}
	}

	return traces, nil
}
