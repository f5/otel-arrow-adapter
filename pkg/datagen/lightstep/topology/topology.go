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

import "fmt"

type Topology struct {
	Services map[string]*ServiceTier `json:"services" yaml:"services"`
}

func (t *Topology) GetServiceTier(serviceName string) *ServiceTier {
	return t.Services[serviceName]
}

func (t *Topology) ValidateServiceGraph(rootRoutes []RootRoute) error {
	for _, rr := range rootRoutes {
		err := t.validateDownstreamCalls(rr.Service, rr.Route)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Topology) validateDownstreamCalls(service string, route string) error {
	seenCalls := map[string]bool{service + route: true}
	orderedCalls := []string{service + route}
	err := t.traverseServiceGraph(service, route, seenCalls, orderedCalls)
	// TODO: optimize so that we don't check same service/route combo twice
	if err != nil {
		return err
	}
	return nil
}

func (t *Topology) traverseServiceGraph(service string, route string, seenCalls map[string]bool, orderedCalls []string) error {
	downstreamCalls := t.GetServiceTier(service).GetRoute(route).DownstreamCalls
	// already validated existence of all services/routes, so ^ is safe
	seenCalls[service+route] = true
	for _, c := range downstreamCalls {
		if seenCalls[c.Service+c.Route] {
			return fmt.Errorf(printServiceCycle(orderedCalls, c.Service+c.Route))
		}

		err := t.traverseServiceGraph(c.Service, c.Route, seenCalls, append(orderedCalls, c.Service+c.Route))
		if err != nil {
			return err
		}
	}
	delete(seenCalls, service+route)
	return nil
}

func printServiceCycle(seenCalls []string, repeated string) string {
	var s string
	for _, call := range seenCalls {
		s += fmt.Sprintf("%s -> ", call)
	}
	s += repeated
	return s
}

func (t *Topology) Load() error {
	for name, service := range t.Services {
		err := service.load(name)
		if err != nil {
			return err
		}
	}
	return nil
}
