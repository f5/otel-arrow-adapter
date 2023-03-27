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

package lightstep

import (
	"math/rand"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"

	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep/flags"
)

func TestTraceGenerator(t *testing.T) {
	topoFile, err := ParseTopoFile("/Users/L.Querel/GolandProjects/otel-arrow-adapter/data/hipster_shop.yaml")
	require.NoError(t, err)

	flags.Manager.LoadFlags(topoFile.Flags)

	err = topoFile.Topology.Load()
	require.NoError(t, err)

	var randomSeed int64 = 1
	generatorRand := rand.New(rand.NewSource(randomSeed))

	var tickers []*time.Ticker

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

	for _, rootRoute := range topoFile.RootRoutes {
		traceTicker := time.NewTicker(time.Duration(360000/rootRoute.TracesPerHour) * time.Millisecond)
		tickers = append(tickers, traceTicker)
		svc := rootRoute.Service
		route := rootRoute.Route

		// rand.Rand is not safe to use in different go routines,
		// create one for each go routine, but use the generatorRand to
		// generate the seed.
		routeRand := rand.New(rand.NewSource(generatorRand.Int63()))

		traceGen := NewTraceGenerator(topoFile.Topology, routeRand, svc, route)
		traces := traceGen.Generate(time.Now().UnixNano())

		spew.Dump(traces)
	}
}
