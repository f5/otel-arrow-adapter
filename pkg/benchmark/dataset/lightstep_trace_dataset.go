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

package dataset

import (
	"math/rand"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/f5/otel-arrow-adapter/pkg/benchmark/stats"
	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep"
	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep/flags"
)

type LightstepTracesDataset struct {
	traceCount            int
	currentTraceGenerator int
	traceGenerators       []*lightstep.TraceGenerator
	traces                []*ptrace.Traces
	tracesStats           *stats.TracesStats
}

func NewLightstepTracesDataset(topoFileName string, traceCount int) *LightstepTracesDataset {
	topoFile, err := lightstep.ParseTopoFile(topoFileName)
	if err != nil {
		panic(err)
	}

	flags.Manager.LoadFlags(topoFile.Flags)

	err = topoFile.Topology.Load()
	if err != nil {
		panic(err)
	}

	var randomSeed int64 = 1
	generatorRand := rand.New(rand.NewSource(randomSeed))

	for _, s := range topoFile.Topology.Services {
		for i := range s.ResourceAttributeSets {
			k := s.ResourceAttributeSets[i].Kubernetes
			if k == nil {
				continue
			}
			k.Cfg = topoFile.Config
			k.CreatePods(s.ServiceName)
		}
	}

	var traceGenerators []*lightstep.TraceGenerator
	for _, rootRoute := range topoFile.RootRoutes {
		svc := rootRoute.Service
		route := rootRoute.Route

		// rand.Rand is not safe to use in different go routines,
		// create one for each go routine, but use the generatorRand to
		// generate the seed.
		routeRand := rand.New(rand.NewSource(generatorRand.Int63()))

		traceGenerators = append(traceGenerators, lightstep.NewTraceGenerator(topoFile.Topology, routeRand, svc, route))
	}

	time := time.Now().UnixNano()
	var traces []*ptrace.Traces
	for i := 0; i < traceCount; i++ {
		trace := traceGenerators[i%len(traceGenerators)].Generate(time)
		traces = append(traces, trace)
		time += 1000000000
	}

	ds := &LightstepTracesDataset{
		traceCount:            traceCount,
		currentTraceGenerator: 0,
		traceGenerators:       traceGenerators,
		traces:                traces,
		tracesStats:           stats.NewTracesStats(),
	}
	return ds
}

func (d *LightstepTracesDataset) Resize(size int) {
	d.traces = d.traces[:size]
}

func (d *LightstepTracesDataset) SizeInBytes() int {
	return 0
}

func (d *LightstepTracesDataset) Len() int {
	return len(d.traces)
}

func (d *LightstepTracesDataset) ShowStats() {
	println()
	println("Traces stats:")
	d.tracesStats.ShowStats()
}

func (d *LightstepTracesDataset) Traces(offset, size int) []ptrace.Traces {
	var traces []ptrace.Traces
	for i := 0; i < size; i++ {
		traces = append(traces, *d.traces[offset+i])
	}

	return traces
}
