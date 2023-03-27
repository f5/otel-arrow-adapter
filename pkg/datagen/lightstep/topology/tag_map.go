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
	"math/rand"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

type TagMap map[string]interface{}

func (tm *TagMap) InsertTags(attr *pcommon.Map) {
	for key, val := range *tm {
		switch val := val.(type) {
		case float64:
			attr.PutDouble(key, val)
		case int:
			attr.PutInt(key, int64(val))
		case string:
			_, err := strconv.Atoi(val)
			if err != nil {
				attr.PutStr(key, val)
			}
		case bool:
			attr.PutBool(key, val)
		case []string:
			attr.PutStr(key, val[rand.Intn(len(val))])
		default:
			attr.PutStr(key, fmt.Sprint(val))
		}
	}
}
