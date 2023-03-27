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
	"fmt"

	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep/flags"
)

type File struct {
	Topology   *Topology          `json:"topology" yaml:"topology"`
	Flags      []flags.FlagConfig `json:"flags" yaml:"flags"`
	RootRoutes []RootRoute        `json:"rootRoutes" yaml:"rootRoutes"`
	Config     *Config            `json:"config" yaml:"config"`
}

type Config struct {
	Kubernetes *KubernetesConfig
}

type KubernetesConfig struct {
	PodCount int `json:"pod_count" yaml:"pod_count"`
}

type RootRoute struct {
	Service             string `json:"service" yaml:"service"`
	Route               string `json:"route" yaml:"route"`
	TracesPerHour       int    `json:"tracesPerHour" yaml:"tracesPerHour"`
	flags.EmbeddedFlags `json:",inline" yaml:",inline"`
}

func (file *File) ValidateRootRoutes() error {
	for _, rr := range file.RootRoutes {
		st := file.Topology.GetServiceTier(rr.Service)
		if st == nil {
			return fmt.Errorf("service %s does not exist", rr.Service)
		}
		if st.GetRoute(rr.Route) == nil {
			return fmt.Errorf("service %s does not have route %s defined", rr.Service, rr.Route)
		}
		if rr.TracesPerHour <= 0 {
			return fmt.Errorf("rootRoute %s must have a positive, non-zero tracesPerHour defined", rr.Route)
		}
	}
	return nil
}
