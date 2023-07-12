/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/f5/otel-arrow-adapter/pkg/otel/arrow_record"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	flag.Parse()

	producer := arrow_record.NewProducer()
	consumer := arrow_record.NewConsumer()

	for _, file := range flag.Args() {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("open: %s: %w", file, err)
			return
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var un ptrace.JSONUnmarshaler

			expected, err := un.UnmarshalTraces([]byte(scanner.Text()))
			if err != nil {
				log.Fatalf("parse: %w", err)
			}

			fmt.Println("PRODUCING")
			batch, err := producer.BatchArrowRecordsFromTraces(expected)
			if err != nil {
				log.Fatalf("produce arrow: %w", err)
			}

			fmt.Println("CONSUMING")
			received, err := consumer.TracesFrom(batch)
			if err != nil {
				log.Fatalf("consume arrow: %w", err)
			}
			if len(received) != 1 {
				log.Fatalf("expecting 1 traces: %d", len(received))
			}

			// assert.Equiv(t, []json.Marshaler{
			// 	ptraceotlp.NewExportRequestFromTraces(expected),
			// }, []json.Marshaler{
			// 	ptraceotlp.NewExportRequestFromTraces(received[0]),
			// })

			var mar ptrace.JSONMarshaler

			data1, _ := mar.MarshalTraces(expected)
			data2, _ := mar.MarshalTraces(received[0])

			fmt.Println("DATA1", string(data1))
			fmt.Println("DATA2", string(data2))
		}
		if err := scanner.Err(); err != nil {
			log.Fatalf("read: %w", err)
		}
	}
}
