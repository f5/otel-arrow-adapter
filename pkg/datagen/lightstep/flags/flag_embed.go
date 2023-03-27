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
	"time"
)

type EmbeddedFlags struct {
	FlagSet   string `json:"flag_set" yaml:"flag_set"`
	FlagUnset string `json:"flag_unset" yaml:"flag_unset"`
}

func (f EmbeddedFlags) ShouldGenerate() bool {
	// TODO: use the set flag's _value_... somehow
	if f.FlagSet != "" {
		if set := Manager.GetFlag(f.FlagSet); !set.Active() {
			return false
		}
	}
	if f.FlagUnset != "" {
		if unset := Manager.GetFlag(f.FlagUnset); unset.Active() {
			return false
		}
	}
	return true
}

func (f EmbeddedFlags) IsDefault() bool {
	return f.FlagSet == "" && f.FlagUnset == ""
}

func (f EmbeddedFlags) GenerateStartTime() time.Time {
	if !f.ShouldGenerate() {
		return time.UnixMilli(0)
	}

	s, u := time.UnixMilli(0), time.UnixMilli(0)

	if f.FlagSet != "" {
		s = Manager.GetFlag(f.FlagSet).updated
	}

	if f.FlagUnset != "" {
		u = Manager.GetFlag(f.FlagUnset).updated
	}

	if s.After(u) {
		return s
	}

	return u
}

func (f EmbeddedFlags) ValidateFlags() error {
	if f.FlagSet != "" && Manager.GetFlag(f.FlagSet) == nil {
		return fmt.Errorf("flag %v does not exist", f.FlagSet)
	}
	if f.FlagUnset != "" && Manager.GetFlag(f.FlagUnset) == nil {
		return fmt.Errorf("flag %v does not exist", f.FlagUnset)
	}
	return nil
}
