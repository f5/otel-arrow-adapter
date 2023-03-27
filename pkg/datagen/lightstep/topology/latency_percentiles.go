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

package topology

import (
	"math/rand"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep/flags"
)

type LatencyPercentiles struct {
	P0Cfg     string `json:"p0" yaml:"p0"`
	P50Cfg    string `json:"p50" yaml:"p50"`
	P95Cfg    string `json:"p95" yaml:"p95"`
	P99Cfg    string `json:"p99" yaml:"p99"`
	P999Cfg   string `json:"p99.9" yaml:"p99.9"`
	P100Cfg   string `json:"p100" yaml:"p100"`
	durations struct {
		p0   time.Duration
		p50  time.Duration
		p95  time.Duration
		p99  time.Duration
		p999 time.Duration
		p100 time.Duration
	}
	EmbeddedWeight      `json:",inline" yaml:",inline"`
	flags.EmbeddedFlags `json:",inline" yaml:",inline"`
}

func (l *LatencyPercentiles) Sample() int64 {
	if l == nil {
		// This results from having a list where
		// items are !ShouldGenerate() which leaves
		// an empty list, the zeroP is returned.
		return 0
	}
	uniform := func(timeA, timeB time.Duration) int64 {
		min := float64(timeA.Nanoseconds())
		max := float64(timeB.Nanoseconds())
		return int64(min + (max-min)*rand.Float64())
	}
	genNumber := rand.Float64()
	switch {
	case genNumber < 0.5:
		return uniform(l.durations.p0, l.durations.p50)
	case genNumber < 0.95:
		return uniform(l.durations.p50, l.durations.p95)
	case genNumber < 0.99:
		return uniform(l.durations.p95, l.durations.p99)
	case genNumber < 0.999:
		return uniform(l.durations.p99, l.durations.p999)
	default:
		return uniform(l.durations.p999, l.durations.p100)
	}
}

func (l *LatencyPercentiles) loadDurations() error {
	// TODO/future things:
	// 		normalize function for config parsing
	// 		maybe enforce either MaxLatencyMillis or LatencyConfigs but not both?
	//			either way which overrides which? for now LatencyConfigs will override MaxLatencyMillis
	var err error
	l.durations.p0, err = time.ParseDuration(l.P0Cfg)
	if err != nil {
		return err
	}
	l.durations.p50, err = time.ParseDuration(l.P50Cfg)
	if err != nil {
		return err
	}
	l.durations.p95, err = time.ParseDuration(l.P95Cfg)
	if err != nil {
		return err
	}
	l.durations.p99, err = time.ParseDuration(l.P99Cfg)
	if err != nil {
		return err
	}
	l.durations.p999, err = time.ParseDuration(l.P999Cfg)
	if err != nil {
		return err
	}
	l.durations.p100, err = time.ParseDuration(l.P100Cfg)
	if err != nil {
		return err
	}
	return nil
}

type LatencyConfigs []*LatencyPercentiles

func (lcfg *LatencyConfigs) Sample(traceID pcommon.TraceID) int64 {
	var defaultCfg *LatencyPercentiles
	var enabled []*LatencyPercentiles
	for _, cfg := range *lcfg {
		if cfg.IsDefault() {
			defaultCfg = cfg
		} else if cfg.ShouldGenerate() {
			enabled = append(enabled, cfg)
		}
	}
	if len(enabled) > 0 {
		picked := pickBasedOnWeight(enabled, traceID)

		if picked != nil {
			return picked.Sample()
		}
	}
	return defaultCfg.Sample()
}
