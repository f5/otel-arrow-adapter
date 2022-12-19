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

package arrow

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
)

type LogCompressor struct {
	config      *common.LogConfig
	delimiter   *regexp.Regexp
	dictVars    []*regexp.Regexp
	nonDictVars []*regexp.Regexp
}

func NewLogCompressor(config *common.LogConfig) *LogCompressor {
	var (
		delimiter *regexp.Regexp
		dictVars  []*regexp.Regexp
	)

	if config != nil {
		delimiter = regexp.MustCompile(fmt.Sprintf("[%s]+", config.Delimiter))

		dictVars = make([]*regexp.Regexp, len(config.DictVars))
		for i, pattern := range config.DictVars {
			dictVars[i] = regexp.MustCompile(fmt.Sprintf("^%s$", pattern))
		}
	}

	return &LogCompressor{
		config:    config,
		delimiter: delimiter,
		dictVars:  dictVars,
	}
}

func (lc *LogCompressor) Config() *common.LogConfig {
	return lc.config
}

func (lc *LogCompressor) Compress(log string) *common.EncodedLog {
	if lc.config == nil {
		return &common.EncodedLog{
			LogType: log,
		}
	}

	var (
		logTypeBuf strings.Builder
		tokenBuf   strings.Builder
		dictVars   []string
		intVars    []int64
		floatVars  []float64
	)

	delimiterIndices := lc.delimiter.FindAllStringIndex(log, -1)

	if len(delimiterIndices) == 0 {
		// No delimiters found, return the whole log as a log type
		return &common.EncodedLog{
			LogType: log,
		}
	}

	// Add an artificial delimiter at the end of the log to simplify
	// the logic of the main loop.
	delimiterIndices = append(delimiterIndices, []int{len(log), len(log)})

	curDelimiter := 0
	inToken := true
	if delimiterIndices[curDelimiter][0] == 0 {
		inToken = false
	}

	// iterate over the log characters (single pass)
	for pos, c := range log {
		if c == '\x11' || c == '\x12' {
			// skip these characters
			continue
		}

		if pos == delimiterIndices[curDelimiter][0] { // left delimiter
			token := tokenBuf.String()
			tokenBuf.Reset()
			if varFound := lc.extractVariable(&token, &logTypeBuf, &intVars, &floatVars, &dictVars); !varFound {
				logTypeBuf.WriteString(token)
			}
			inToken = false
		} else if pos == delimiterIndices[curDelimiter][1] { // right delimiter
			curDelimiter++
			inToken = true
		}

		// Write current character to the current segment (token or log type)
		if inToken {
			tokenBuf.WriteRune(c)
		} else {
			logTypeBuf.WriteRune(c)
		}
	}

	// In case of a trailing token
	if tokenBuf.Len() > 0 {
		token := tokenBuf.String()
		if varFound := lc.extractVariable(&token, &logTypeBuf, &intVars, &floatVars, &dictVars); !varFound {
			logTypeBuf.WriteString(token)
		}
	}

	return &common.EncodedLog{
		LogType:   logTypeBuf.String(),
		DictVars:  dictVars,
		IntVars:   intVars,
		FloatVars: floatVars,
	}
}

func (lc *LogCompressor) extractVariable(text *string, logType *strings.Builder, intVars *[]int64, floatVars *[]float64, dictVars *[]string) bool {
	if i64V, err := strconv.ParseInt(*text, 10, 64); err == nil {
		logType.WriteRune('\x12')
		logType.WriteRune(rune(len(*intVars)))
		*intVars = append(*intVars, i64V)
		return true
	}

	if f64V, err := strconv.ParseFloat(*text, 64); err == nil {
		logType.WriteRune('\x13')
		logType.WriteRune(rune(len(*floatVars)))
		*floatVars = append(*floatVars, f64V)
		return true
	}

	for _, regex := range lc.dictVars {
		if regex.Match([]byte(*text)) {
			logType.WriteRune('\x11')
			logType.WriteRune(rune(len(*dictVars)))
			*dictVars = append(*dictVars, *text)
			return true
		}
	}

	return false
}
