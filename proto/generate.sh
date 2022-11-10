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
go install github.com/vektra/mockery/v2@latest

mkdir -p api/collector/arrow/v1/mock
mkdir -p pkg/otel/arrow_record/mock

# mocks in pkg/otel/arrow_record
ARROW_RECORD_MOCKS="ProducerAPI ConsumerAPI"

for name in ${ARROW_RECORD_MOCKS}; do
    mockery --dir ./pkg/otel/arrow_record --output ./pkg/otel/arrow_record/mocks --name ${name}
done

# mocks in api/collector/arrow/v1
ARROW_COLLECTOR_API_MOCKS="ArrowStreamServiceClient ArrowStreamService_ArrowStreamClient ArrowStreamServiceServer ArrowStreamService_ArrowStreamServer"

for name in ${ARROW_COLLECTOR_API_MOCKS}; do
    mockery --dir ./api/collector/arrow/v1 --output ./api/collector/arrow/v1/mocks --name ${name}
done

go mod tidy
