#!/usr/bin/env bash

# Run this in the top-level directory.
rm -rf api
mkdir api

# Get current directory.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

for dir in $(find ${DIR}/opentelemetry -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq); do
  files=$(find "${dir}" -name '*.proto')

  # Generate all files with protoc-gen-go.
  echo ${files}
  protoc -I ${DIR} --go_out=api --go-grpc_out=api ${files}
done

mv api/github.com/f5/otel-arrow-adapter/api/collector api
rm -rf api/github.com

# Generate the mock files
go install github.com/golang/mock@latest
go get github.com/golang/mock@latest
mkdir -p api/collector/arrow/v1/mock
mockgen -package mock github.com/f5/otel-arrow-adapter/api/collector/arrow/v1 ArrowStreamServiceClient,ArrowStreamService_ArrowStreamClient > api/collector/arrow/v1/mock/arrow_service_mock.go
go mod tidy
