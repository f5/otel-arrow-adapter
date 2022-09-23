REMOVALS = \
	api/go.opentelemetry.io/proto/otlp/collector/logs \
	api/go.opentelemetry.io/proto/otlp/collector/metrics \
	api/go.opentelemetry.io/proto/otlp/collector/trace \
	api/go.opentelemetry.io/proto/otlp/common \
	api/go.opentelemetry.io/proto/otlp/logs \
	api/go.opentelemetry.io/proto/otlp/metrics \
	api/go.opentelemetry.io/proto/otlp/resource \
	api/go.opentelemetry.io/proto/otlp/trace \
	proto/opentelemetry/proto/collector/README.md \
	proto/opentelemetry/proto/collector/logs \
	proto/opentelemetry/proto/collector/metrics \
	proto/opentelemetry/proto/collector/trace \
	proto/opentelemetry/proto/common \
	proto/opentelemetry/proto/logs \
	proto/opentelemetry/proto/metrics \
	proto/opentelemetry/proto/resource \
	proto/opentelemetry/proto/trace

GO_FILES := $(shell find . -name '*.go')

# Function to execute a command. Note the empty line before endef to make sure each command
# gets executed separately instead of concatenated with previous one.
# Accepts command to execute as first parameter.
define exec-command
$(1)

endef

all:
	echo "Run 'make gen'"
gen:
	#git checkout -- .
	#rm -rf ${REMOVALS}
	$(MAKE) subgen

subgen:
	$(foreach file, $(GO_FILES),$(call exec-command,sed -i '' -f patch.sed $(file)))
	go mod tidy
	go build ./...
