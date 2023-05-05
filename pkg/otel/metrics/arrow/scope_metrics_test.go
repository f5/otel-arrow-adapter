// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common"
)

func TestIntersectAttrs(t *testing.T) {
	t.Parallel()

	initMap := pcommon.NewMap()
	initMap.PutStr("a", "1")
	initMap.PutStr("b", "2")
	initMap.PutInt("c", 3)
	initMap.PutDouble("d", 4.0)
	initMap.PutBool("e", true)
	initMap.PutEmptyBytes("f").Append([]byte("6")...)
	initMap.PutEmptyMap("g").PutStr("g1", "7")
	sharedAttrs := common.NewSharedAttributesFrom(initMap)

	// All attributes in common
	attrs := pcommon.NewMap()
	attrs.PutStr("a", "1")
	attrs.PutStr("b", "2")
	attrs.PutInt("c", 3)
	attrs.PutDouble("d", 4.0)
	attrs.PutBool("e", true)
	attrs.PutEmptyBytes("f").Append([]byte("6")...)
	attrs.PutEmptyMap("g").PutStr("g1", "7")
	sharedAttrsCount := sharedAttrs.IntersectWithMap(attrs)
	require.Equal(t, 7, sharedAttrsCount)

	// 1 attribute is missing from attrs
	attrs = pcommon.NewMap()
	attrs.PutStr("a", "1")
	attrs.PutStr("b", "2")
	attrs.PutInt("c", 3)
	attrs.PutDouble("d", 4.0)
	attrs.PutBool("e", true)
	attrs.PutEmptyBytes("f").Append([]byte("6")...)
	sharedAttrsCount = sharedAttrs.IntersectWithMap(attrs)
	require.Equal(t, 6, sharedAttrsCount)
	require.False(t, sharedAttrs.Has("g"))

	// 1 attribute has a different
	attrs = pcommon.NewMap()
	attrs.PutStr("a", "1")
	attrs.PutStr("b", "2")
	attrs.PutInt("c", 3)
	attrs.PutDouble("d", 4.0)
	attrs.PutBool("e", false)
	attrs.PutEmptyBytes("f").Append([]byte("6")...)
	sharedAttrsCount = sharedAttrs.IntersectWithMap(attrs)
	require.Equal(t, 5, sharedAttrsCount)
	require.False(t, sharedAttrs.Has("e"))

	// 1 attribute is new
	attrs = pcommon.NewMap()
	attrs.PutStr("a", "1")
	attrs.PutStr("b", "2")
	attrs.PutInt("c", 3)
	attrs.PutDouble("d", 4.0)
	attrs.PutEmptyBytes("f").Append([]byte("6")...)
	attrs.PutBool("h", false)
	sharedAttrsCount = sharedAttrs.IntersectWithMap(attrs)
	require.Equal(t, 5, sharedAttrsCount)
	require.False(t, sharedAttrs.Has("h"))

	// 1 attribute is new
	// 1 attribute is missing
	// 1 attribute has a different value
	attrs = pcommon.NewMap()
	attrs.PutStr("a", "1")
	attrs.PutStr("b", "2")
	attrs.PutInt("c", 4)
	attrs.PutEmptyBytes("f").Append([]byte("6")...)
	attrs.PutBool("h", false)
	sharedAttrsCount = sharedAttrs.IntersectWithMap(attrs)
	require.Equal(t, 3, sharedAttrsCount)
	require.True(t, sharedAttrs.Has("a"))
	require.True(t, sharedAttrs.Has("b"))
	require.True(t, sharedAttrs.Has("f"))

	// No attributes in common
	attrs = pcommon.NewMap()
	attrs.PutStr("x", "1")
	attrs.PutStr("y", "2")
	sharedAttrsCount = sharedAttrs.IntersectWithMap(attrs)
	require.Equal(t, 0, sharedAttrsCount)

	// Empty attributes
	attrs = pcommon.NewMap()
	sharedAttrsCount = sharedAttrs.IntersectWithMap(attrs)
	require.Equal(t, 0, sharedAttrsCount)
}
