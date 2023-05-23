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

// General configuration for metrics. This configuration defines the different
// sorters used for attributes, metrics, data points, ... and the encoding used
// for parent IDs.

type (
	Config struct {
		Global *cfg.Config

		Metric *MetricConfig
		//Sum          *SumConfig
		//Gauge        *GaugeConfig
		//Summary      *SummaryConfig
		//Histogram    *HistogramConfig
		//ExpHistogram *ExpHistogramConfig

		Attrs *AttrsConfig
	}

	AttrsConfig struct {
		Resource     *arrow.Attrs16Config
		Scope        *arrow.Attrs16Config
		Sum          *arrow.Attrs32Config
		Gauge        *arrow.Attrs32Config
		Summary      *arrow.Attrs32Config
		Histogram    *arrow.Attrs32Config
		ExpHistogram *arrow.Attrs32Config
	}

	MetricConfig struct {
		Sorter MetricSorter
	}

	//SumConfig struct {
	//	Sorter           SumSorter
	//}
	//
	//GaugeConfig struct {
	//	Sorter           GaugeSorter
	//}
	//
	//SummaryConfig struct {
	//	Sorter           SummarySorter
	//}
	//
	//HistogramConfig struct {
	//	Sorter           HistogramSorter
	//}
	//
	//ExpHistogramConfig struct {
	//	Sorter           ExpHistogramSorter
	//}
)
