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
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
)

func TestCompress(t *testing.T) {
	compressor := NewLogCompressor(common.DefaultLogConfig())

	encodedLog := compressor.Compress("duration: 134")
	require.Equal(t, "duration: \x12\x00", encodedLog.LogType)
	require.Equal(t, 0, len(encodedLog.DictVars))
	require.Equal(t, 1, len(encodedLog.IntVars))
	require.EqualValues(t, []int64{134}, encodedLog.IntVars)
	require.Equal(t, 0, len(encodedLog.FloatVars))

	encodedLog = compressor.Compress("duration: 134.67")
	require.Equal(t, "duration: \x13\x00", encodedLog.LogType)
	require.Equal(t, 0, len(encodedLog.DictVars))
	require.Equal(t, 0, len(encodedLog.IntVars))
	require.Equal(t, 1, len(encodedLog.FloatVars))
	require.EqualValues(t, []float64{134.67}, encodedLog.FloatVars)

	encodedLog = compressor.Compress("service `abc` (method: GET, duration: 134.67, status: 200, task: task-123, uuid: 12345678-1234-1234-1234-123456789012)")
	require.Equal(t, "service `abc` (method: GET, duration: \x13\x00, status: \x12\x00, task: \x11\x00, uuid: \x11\x01)", encodedLog.LogType)
	require.Equal(t, 2, len(encodedLog.DictVars))
	require.EqualValues(t, []string{"task-123", "12345678-1234-1234-1234-123456789012"}, encodedLog.DictVars)
	require.Equal(t, 1, len(encodedLog.IntVars))
	require.EqualValues(t, []int64{200}, encodedLog.IntVars)
	require.Equal(t, 1, len(encodedLog.FloatVars))
	require.EqualValues(t, []float64{134.67}, encodedLog.FloatVars)

	encodedLog = compressor.Compress("service `abc` ()")
	require.Equal(t, "service `abc` ()", encodedLog.LogType)
	require.Equal(t, 0, len(encodedLog.DictVars))
	require.Equal(t, 0, len(encodedLog.IntVars))
	require.Equal(t, 0, len(encodedLog.FloatVars))

	encodedLog = compressor.Compress("")
	require.Equal(t, "", encodedLog.LogType)
	require.Equal(t, 0, len(encodedLog.DictVars))
	require.Equal(t, 0, len(encodedLog.IntVars))
	require.Equal(t, 0, len(encodedLog.FloatVars))

	encodedLog = compressor.Compress(" \t\n")
	require.Equal(t, " \t\n", encodedLog.LogType)
	require.Equal(t, 0, len(encodedLog.DictVars))
	require.Equal(t, 0, len(encodedLog.IntVars))
	require.Equal(t, 0, len(encodedLog.FloatVars))

	encodedLog = compressor.Compress(" 134 123.0 1234.")
	spew.Dump(encodedLog)
	require.Equal(t, " \\x12\\x00 \\x13\\x00 \\x13\\x01.", encodedLog.LogType)
	require.Equal(t, 0, len(encodedLog.DictVars))
	require.Equal(t, 2, len(encodedLog.IntVars))
	require.EqualValues(t, []int64{134, 1234}, encodedLog.IntVars)
	require.Equal(t, 1, len(encodedLog.FloatVars))
	require.EqualValues(t, []float64{123.0}, encodedLog.FloatVars)

	// TODO
	// - Can't we just extract integers?
	// - Don't think that the number behind the \x13, \x12 is required.
	// - Complete the test and the integration with both side Compress and Decompress
	// - Rename Compress/Decompress to something that express the concept of static pattern extraction
	// - Measure the performance of the compression
}
