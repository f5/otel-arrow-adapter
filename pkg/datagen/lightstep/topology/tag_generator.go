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

import "math/rand"

type TagGenerator struct {
	ValLength int `json:"valLength,omitempty" yaml:"valLength,omitempty"`
	NumTags   int `json:"numTags,omitempty" yaml:"numTags,omitempty"`
	NumVals   int `json:"numVals,omitempty" yaml:"numVals,omitempty"`
	Random    *rand.Rand
}

func (t *TagGenerator) GenerateTags() map[string]string {
	nameGenerator := &TagNameGenerator{
		random: t.Random,
	}

	retVal := make(map[string]string)
	for i := 0; i < t.NumTags; i++ {
		retVal[nameGenerator.Generate()] = randStringBytes(t.ValLength, t.Random)
	}
	return retVal
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int, r *rand.Rand) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[r.Intn(len(letterBytes))]
	}
	return string(b)
}
