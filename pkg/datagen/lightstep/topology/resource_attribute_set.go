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
	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep/flags"
)

type ResourceAttributeSet struct {
	Kubernetes          *Kubernetes `json:"kubernetes" yaml:"kubernetes"`
	ResourceAttributes  TagMap      `json:"resourceAttrs,omitempty" yaml:"resourceAttrs,omitempty"`
	EmbeddedWeight      `json:",inline" yaml:",inline"`
	flags.EmbeddedFlags `json:",inline" yaml:",inline"`
}

func (r *ResourceAttributeSet) GetAttributes() *TagMap {
	tm := make(TagMap)
	for k, v := range r.ResourceAttributes {
		tm[k] = v
	}
	if k8s := r.Kubernetes; k8s != nil {
		for k, v := range k8s.GetK8sTags() {
			tm[k] = v
		}
	}
	return &tm
}
