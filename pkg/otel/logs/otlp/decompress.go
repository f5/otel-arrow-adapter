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

package otlp

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
)

func Decompress(seg *common.EncodedLog) string {
	var log strings.Builder
	x11 := false
	x12 := false
	x13 := false

	for _, c := range seg.LogType {
		if !x11 && !x12 && c == '\x11' {
			x11 = true
			continue
		} else if !x11 && !x12 && c == '\x12' {
			x12 = true
			continue
		} else if !x11 && !x12 && c == '\x13' {
			x13 = true
			continue
		} else {
			if x11 {
				log.WriteString(seg.DictVars[int(c)])
				x11 = false
			} else if x12 {
				log.WriteString(strconv.FormatInt(seg.IntVars[int(c)], 10))
				x12 = false
			} else if x13 {
				log.WriteString(fmt.Sprintf("%f", seg.FloatVars[int(c)]))
				x13 = false
			} else {
				log.WriteRune(c)
			}
		}
	}
	return log.String()
}
