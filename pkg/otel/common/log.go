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

package common

// LogConfig is a configuration for the log compressor (CLP inspired log optimization).
// See [CLP: Efficient and Scalable Search on Compressed Text Logs](https://www.usenix.org/system/files/osdi21-rodrigues.pdf)
type LogConfig struct {
	Delimiter string
	DictVars  []string
}

func DefaultLogConfig() *LogConfig {
	return &LogConfig{
		Delimiter: " \\t\\r\\n!\"#$%&'\\(\\)\\*,:;<>?@\\[\\]\\^_`\\{\\|\\}~",
		DictVars: []string{
			"0x[0-9a-fA-F]+", // Hexadecimal identifier
			"\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}",      // ip v4 address
			"\\[([0-9a-fA-F]{1,4}:){7}([0-9a-fA-F]){1,4}\\]", // ip v6 address
			".+\\d.+",            // words and identifiers ending by a number (e.g. task-123, container-123)
			".*=.*[a-zA-Z0-9].*", // key=value pair with alphanumeric value
		},
	}
}

// EncodedLog represents a log record into 3 parts:
// - LogType: the log type, which is the static pattern detected by the compressor (highly repetitive).
// - DictVars: the dictionary variables, which are the variables that are repeated in the log.
// - IntVars: non-dictionary variables which are int 64 numbers (potentially high cardinality values).
// - FloatVars: non-dictionary variables which are float 64 numbers (potentially high cardinality values).
type EncodedLog struct {
	LogType   string
	DictVars  []string
	IntVars   []int64
	FloatVars []float64
}
