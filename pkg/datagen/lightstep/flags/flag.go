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

package flags

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep/cron"
)

// TODO: separate config types from code types generally

type IncidentConfig struct {
	ParentFlag string        `json:"parentFlag" yaml:"parentFlag"`
	Start      Start         `json:"start" yaml:"start"`
	Duration   time.Duration `json:"duration" yaml:"duration"`
}

type Start []time.Duration

type CronConfig struct {
	Start string `json:"start" yaml:"start"`
	End   string `json:"end" yaml:"end"`
}

type FlagConfig struct {
	Name     string          `json:"name" yaml:"name"`
	Incident *IncidentConfig `json:"incident" yaml:"incident"`
	Cron     *CronConfig     `json:"cron" yaml:"cron"`
}

type Flag struct {
	cfg     FlagConfig
	started time.Time
	updated time.Time
	mu      sync.Mutex
}

func NewFlag(cfg FlagConfig) Flag {
	return Flag{cfg: cfg}
}

func (f *Flag) Name() string {
	return f.cfg.Name
}

func (f *Flag) Active() bool {
	f.update()
	return f.active()
}

func (f *Flag) active() bool {
	return !f.started.IsZero()
}

// update checks if the given flag f has a parent flag ("Incident"); if so,
// updates f's state based on its start and end times relative to the parent.
func (f *Flag) update() {
	if !f.parentSpecified() {
		// managed by cron or manual-only
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	parent := f.parent() // won't be nil because we already validated all parents exist
	incidentDuration := parent.CurrentDuration()
	if f.active() != f.shouldBeActive(incidentDuration) {
		f.Toggle()
	}
}

func (f *Flag) shouldBeActive(incidentDuration time.Duration) bool {
	childDuration := f.cfg.Incident.Duration
	for _, start := range f.cfg.Incident.Start {
		if incidentDuration <= start { // relies on start times being in increasing order (verified in flag validation)
			return false
		}
		// if childDuration not specified, then child flag stays active until end of incident
		if incidentDuration < start+childDuration || childDuration == 0 {
			return true
		}
	}
	return false
}

func (f *Flag) CurrentDuration() time.Duration {
	if !f.active() {
		return 0
	}
	return time.Since(f.started)
}

func (f *Flag) Enable() {
	if !f.active() {
		f.started = time.Now()
		f.updated = time.Now()
	}
}

func (f *Flag) Disable() {
	if f.active() {
		f.started = time.Time{}
		f.updated = time.Now()
	}
}

func (f *Flag) Toggle() {
	if f.active() {
		f.Disable()
	} else {
		f.Enable()
	}
}

func (f *Flag) Setup() {
	// TODO: add validation to disallow having cron and incident both configured?
	if f.cfg.Cron != nil {
		f.SetupCron()
	}
}

func (f *Flag) SetupCron() {
	_, err := cron.Add(f.cfg.Cron.Start, func() {
		f.Enable()
	})
	if err != nil {
		panic(err)
	}

	_, err = cron.Add(f.cfg.Cron.End, func() {
		f.Disable()
	})
	if err != nil {
		panic(err)
	}
}

func (f *Flag) parentSpecified() bool {
	return f.cfg.Incident != nil
}

func (f *Flag) parent() *Flag {
	if !f.parentSpecified() {
		return nil
	}
	return Manager.GetFlag(f.cfg.Incident.ParentFlag)
}

func (ic IncidentConfig) validate() error {
	if Manager.GetFlag(ic.ParentFlag) == nil {
		return fmt.Errorf("parent flag %s does not exist", ic.ParentFlag)
	}
	if len(ic.Start) == 0 {
		return fmt.Errorf("start cannot be empty")
	}
	if ic.Duration == 0 && len(ic.Start) > 1 {
		return fmt.Errorf("if duration is not specified, only one start time is permitted (will last until end of incident)")
	}
	previousStart := time.Duration(-1)
	for _, start := range ic.Start {
		if start <= previousStart {
			return fmt.Errorf("start times must be in strictly increasing order")
		}
		previousStart = start
	}
	return nil
}

// UnmarshalYAML is a custom unmarshaller that parses a comma separated string into a slice of durations
func (s *Start) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var startStr string
	if err := unmarshal(&startStr); err != nil {
		return err
	}
	startTimes := strings.Split(startStr, ",")
	for _, st := range startTimes {
		st = strings.TrimSpace(st)
		stDuration, err := time.ParseDuration(st)
		if err != nil {
			return err
		}
		*s = append(*s, stDuration)
	}
	return nil
}
