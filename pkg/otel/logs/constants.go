package logs

import "github.com/lquerel/otel-arrow-adapter/pkg/otel/constants"

type Constants struct{}

func (_ Constants) ResourceEntitiesLabel() string {
	return constants.RESOURCE_LOGS
}
func (_ Constants) ScopeEntitiesLabel() string {
	return constants.SCOPE_LOGS
}
func (_ Constants) EntitiesLabel() string {
	return constants.LOGS
}
