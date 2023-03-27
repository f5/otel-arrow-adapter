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
	"math"
	"math/rand"
	"time"

	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep/flags"
)

const DefaultPeriod = 60 * time.Minute
const DefaultOffset = 0 * time.Minute
const DefaultMetricTickerPeriod = 15 * time.Second

type ShapeInterface interface {
	GetValue(phase float64) float64
}

type funcShape struct {
	shape func(phase float64) float64
}

func (fs *funcShape) GetValue(phase float64) float64 {
	return fs.shape(phase)
}

type leakingShape struct {
	average ShapeInterface
	pod     *Pod
}

func (ls *leakingShape) GetValue(phase float64) float64 {
	if ls.pod == nil {
		return ls.average.GetValue(phase)
	}

	timeAlive := time.Since(ls.pod.StartTime)

	// Start at 35% and increase it to 100% right before the restart time.
	factor := .35 + float64(timeAlive)/float64(ls.pod.RestartDuration)*.7

	return factor
}

type Shape string

const (
	Sine     Shape = "sine"
	Sawtooth Shape = "sawtooth"
	Square   Shape = "square"
	Triangle Shape = "triangle"
	Average  Shape = "average"
	Leaking  Shape = "leaking"
)

type Metric struct {
	Name                string            `json:"name" yaml:"name"`
	Type                string            `json:"type" yaml:"type"`
	Min                 float64           `json:"min" yaml:"min"`
	Max                 float64           `json:"max" yaml:"max"`
	Period              *time.Duration    `json:"period" yaml:"period"`
	Offset              *time.Duration    `json:"offset" yaml:"offset"`
	Shape               Shape             `json:"shape" yaml:"shape"`
	ShapeInterface      ShapeInterface    `json:"-" yaml:"-"`
	Tags                map[string]string `json:"tags" yaml:"tags"`
	Jitter              float64           `json:"jitter" yaml:"jitter"`
	flags.EmbeddedFlags `json:",inline" yaml:",inline"`
	Pod                 *Pod
}

func (m *Metric) GetTags() map[string]string {
	if m.Pod != nil {
		return m.Pod.ReplaceTags(m.Tags)
	}

	return m.Tags
}

func (m *Metric) InitMetric() {
	if m.ShapeInterface != nil {
		return
	}

	switch m.Shape {
	case Sine:
		m.ShapeInterface = &funcShape{SineValue}
	case Sawtooth:
		m.ShapeInterface = &funcShape{SawtoothValue}
	case Square:
		m.ShapeInterface = &funcShape{SquareValue}
	case Triangle:
		m.ShapeInterface = &funcShape{TriangleValue}
	case Average:
		m.ShapeInterface = &funcShape{AverageValue}
	case Leaking:
		m.ShapeInterface = &leakingShape{average: &funcShape{AverageValue}, pod: m.Pod}
	default:
		// TODO: what would be a reasonable default? Maybe just sine?
		m.ShapeInterface = &funcShape{SineValue}

	}
}

func SineValue(phase float64) float64 {
	return (math.Sin(2*math.Pi*phase) + 1) / 2
}

func SawtoothValue(phase float64) float64 {
	return phase
}

func SquareValue(phase float64) float64 {
	if phase < 0.5 {
		return 0.0
	}
	return 1.0
}

func TriangleValue(phase float64) float64 {
	return 1.0 - 2.0*math.Abs(0.5-phase)
}

func AverageValue(_ float64) float64 {
	return 0.5
}

func (m *Metric) GetValue() float64 {
	if m.Period == nil {
		period := DefaultPeriod
		m.Period = &period
	}
	if m.Offset == nil {
		offset := DefaultOffset
		m.Offset = &offset
	}

	now := time.Now().Add(-*m.Offset)
	since := now.Sub(now.Truncate(*m.Period))
	phase := float64(since) / float64(*m.Period)

	if m.ShapeInterface == nil {
		m.InitMetric()
	}

	factor := m.ShapeInterface.GetValue(phase)

	v := m.Min + (m.Max-m.Min)*factor

	// jitter deviation is calculated in percentage that ranges from [-m.Jitter/2, m.Jitter/2)%
	j := 1 + rand.Float64()*m.Jitter - m.Jitter/2

	v = v * j

	// ensures value is on the [m.Min, m.Max] boundary
	v = math.Min(v, m.Max)
	v = math.Max(v, m.Min)

	return v
}
