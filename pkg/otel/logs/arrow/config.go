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
	cfg "github.com/f5/otel-arrow-adapter/pkg/config"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
)

type (
	Config struct {
		Global *cfg.Config

		Log   *LogConfig
		Attrs *AttrsConfig
	}

	AttrsConfig struct {
		Resource *arrow.Attrs16Config
		Scope    *arrow.Attrs16Config
		Log      *arrow.Attrs16Config
	}

	LogConfig struct {
		Sorter LogSorter
	}
)

func DefaultConfig() *Config {
	return NewConfig(cfg.DefaultConfig())
}

func NewConfig(globalConf *cfg.Config) *Config {
	return &Config{
		Global: globalConf,
		Log: &LogConfig{
			Sorter: SortLogsByResourceLogsIDScopeLogsIDTraceID(),
		},
		Attrs: &AttrsConfig{
			Resource: &arrow.Attrs16Config{
				Sorter:           arrow.SortAttrs16ByKeyValueParentId(),
				ParentIdEncoding: arrow.ParentIdDeltaGroupEncoding,
			},
			Scope: &arrow.Attrs16Config{
				Sorter:           arrow.SortAttrs16ByKeyValueParentId(),
				ParentIdEncoding: arrow.ParentIdDeltaGroupEncoding,
			},
			Log: &arrow.Attrs16Config{
				Sorter:           arrow.SortAttrs16ByKeyValueParentId(),
				ParentIdEncoding: arrow.ParentIdDeltaGroupEncoding,
			},
		},
	}
}

func NewNoSortConfig(globalConf *cfg.Config) *Config {
	return &Config{
		Global: globalConf,
		Log: &LogConfig{
			Sorter: UnsortedLogs(),
		},
		Attrs: &AttrsConfig{
			Resource: &arrow.Attrs16Config{
				Sorter:           arrow.UnsortedAttrs16(),
				ParentIdEncoding: arrow.ParentIdNoEncoding,
			},
			Scope: &arrow.Attrs16Config{
				Sorter:           arrow.UnsortedAttrs16(),
				ParentIdEncoding: arrow.ParentIdNoEncoding,
			},
			Log: &arrow.Attrs16Config{
				Sorter:           arrow.UnsortedAttrs16(),
				ParentIdEncoding: arrow.ParentIdNoEncoding,
			},
		},
	}
}
