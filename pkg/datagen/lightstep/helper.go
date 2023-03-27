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
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/f5/otel-arrow-adapter/pkg/datagen/lightstep/topology"
)

func hasAnySuffix(s string, suffixes []string) bool {
	for _, suffix := range suffixes {
		if strings.HasSuffix(s, suffix) {
			return true
		}
	}

	return false
}

func ParseTopoFile(topoPath string) (*topology.File, error) {
	var topo topology.File
	topoFile, err := os.Open(topoPath)
	if err != nil {
		return nil, err
	}
	defer topoFile.Close()

	byteValue, _ := io.ReadAll(topoFile)
	lowerTopoPath := strings.ToLower(topoPath)
	if hasAnySuffix(lowerTopoPath, []string{".yaml", ".yml"}) {
		err = yaml.Unmarshal(byteValue, &topo)
	} else {
		err = fmt.Errorf("Unrecognized topology file type: %s", topoPath)
	}

	if err != nil {
		return nil, err
	}
	return &topo, nil
}
