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
	"sync"
)

type FlagManager struct {
	flags map[string]*Flag

	mu sync.Mutex
}

var Manager *FlagManager

func init() {
	Manager = NewFlagManager()
}

func NewFlagManager() *FlagManager {
	return &FlagManager{flags: make(map[string]*Flag)}
}

func (fm *FlagManager) Clear() {
	fm.mu.Lock()
	fm.flags = make(map[string]*Flag)
	fm.mu.Unlock()
}

func (fm *FlagManager) GetFlags() map[string]*Flag {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.flags
}

func (fm *FlagManager) LoadFlags(configFlags []FlagConfig) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for _, cfg := range configFlags {
		flag := NewFlag(cfg)
		flag.Setup()
		fm.flags[flag.Name()] = &flag
	}
}

func (fm *FlagManager) ValidateFlags() error {
	validatedFlags := make(map[string]bool)
	for _, f := range fm.GetFlags() {
		if !validatedFlags[f.Name()] {
			flagGraph, err := fm.traverseFlagGraph(f)
			if err != nil {
				return err
			}
			for k, v := range flagGraph { // we know these flags are valid, so don't re-check
				validatedFlags[k] = v
			}
		}
	}
	return nil
}

func (fm *FlagManager) traverseFlagGraph(f *Flag) (map[string]bool, error) {
	seenFlags := make(map[string]bool)
	var orderedFlags []string // needed for printing flags in-order if cycle is detected

	for !seenFlags[f.Name()] {
		seenFlags[f.Name()] = true
		orderedFlags = append(orderedFlags, f.Name())
		if !f.parentSpecified() { // no parent specified -> this is a root flag, so we've traversed graph without finding cycle
			return seenFlags, nil
		}
		err := f.cfg.Incident.validate() // this is a child flag, so check that its incident config is valid
		if err != nil {
			return nil, fmt.Errorf("error with flag %s: %v", f.Name(), err)
		}

		f = f.parent()
	}
	return nil, fmt.Errorf("cyclical flag graph detected: %s", printFlagCycle(orderedFlags, f.Name()))
}

func printFlagCycle(seenNames []string, repeated string) (s string) {
	for _, f := range seenNames {
		s += fmt.Sprintf("%s -> ", f)
	}
	return s + repeated
}

func (fm *FlagManager) FlagCount() int {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return len(fm.flags)
}

func (fm *FlagManager) GetFlag(name string) *Flag {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.flags[name]
}
