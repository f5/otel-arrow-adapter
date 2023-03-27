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

	"go.opentelemetry.io/collector/pdata/pcommon"
)

type ServiceTier struct {
	ServiceName           string
	Routes                map[string]*ServiceRoute `json:"routes" yaml:"routes"`
	TagSets               []TagSet                 `json:"tagSets" yaml:"tagSets"`
	ResourceAttributeSets []ResourceAttributeSet   `json:"resourceAttrSets" yaml:"resourceAttrSets"`
	Metrics               []Metric                 `json:"metrics" yaml:"metrics"`
}

func (st *ServiceTier) GetTagSet(routeName string, traceID pcommon.TraceID) TagSet {
	serviceTagSet := pickBasedOnWeight(st.TagSets, traceID)
	routeTagSet := pickBasedOnWeight(st.GetRoute(routeName).TagSets, traceID)

	combinedTags := TagMap{}
	for k, v := range serviceTagSet.Tags {
		combinedTags[k] = v
	}
	for k, v := range routeTagSet.Tags {
		combinedTags[k] = v // if service and route have a duplicate tag, the value from route will override
	}

	return TagSet{
		Tags:          combinedTags,
		TagGenerators: append(serviceTagSet.TagGenerators, routeTagSet.TagGenerators...),
	}
}

func (st *ServiceTier) GetResourceAttributeSet(traceID pcommon.TraceID) ResourceAttributeSet {
	// TODO: also support resource attributes on routes
	return pickBasedOnWeight(st.ResourceAttributeSets, traceID)
}

func (st *ServiceTier) GetRoute(routeName string) *ServiceRoute {
	return st.Routes[routeName]
}

func (st *ServiceTier) Validate(topology Topology) error {
	for _, m := range st.Metrics {
		err := m.ValidateFlags()
		if err != nil {
			return fmt.Errorf("error with metric %s in service %s: %v", m.Name, st.ServiceName, err)
		}
	}
	for _, r := range st.Routes {
		err := r.validate(topology)
		if err != nil {
			return fmt.Errorf("error with route %s in service %s: %v", r.Route, st.ServiceName, err)
		}
	}
	for _, t := range st.TagSets {
		err := t.ValidateFlags()
		if err != nil {
			return fmt.Errorf("error with tagSets in service %s: %v", st.ServiceName, err)
		}
	}
	for i := range st.ResourceAttributeSets {
		err := st.ResourceAttributeSets[i].ValidateFlags()
		if err != nil {
			return fmt.Errorf("error with resourceAttributeSets in service %s: %v", st.ServiceName, err)
		}
	}
	return nil
}

func (st *ServiceTier) load(service string) error {
	st.ServiceName = service
	for i := range st.TagSets {
		err := st.TagSets[i].loadCsvTags()
		if err != nil {
			return fmt.Errorf("error loading csv tags for service %s: %v", service, err)
		}
	}
	for name, route := range st.Routes {
		err := route.load(name)
		if err != nil {
			return fmt.Errorf("error loading route %s for service %s: %v", name, service, err)
		}
	}
	return nil
}
