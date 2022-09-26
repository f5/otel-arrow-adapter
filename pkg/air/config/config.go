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

package config

import "math"

// Config defines configuration for RecordRepository.
type Config struct {
	TraceEncoding TraceEncoding
	Attribute     Attribute

	// Configuration for the dictionaries
	Dictionaries DictionariesConfig
}

type TraceEncoding int8

const (
	Flat TraceEncoding = iota
	Hierarchical
	Hybrid
)

type AttributeEncoding int8

const (
	AttributesAsStructs AttributeEncoding = iota
	AttributesAsLists
	AttributesAsListStructs
)

type Attribute struct {
	Encoding AttributeEncoding
}

// DictionariesConfig defines configuration for binary and string dictionaries.
type DictionariesConfig struct {
	// Dictionary options for binary columns
	BinaryColumns DictionaryConfig

	// Dictionary options for string columns
	StringColumns DictionaryConfig
}

// DictionaryConfig defines configuration for a dictionary.
type DictionaryConfig struct {
	// The creation of a dictionary will be performed only on columns with more than `min_row_count` elements.
	MinRowCount int

	// The creation of a dictionary will be performed only on columns with a cardinality lower than `max_card`.
	MaxCard int

	// The creation of a dictionary will only be performed on columns with a ratio `card` / `size` <= `max_card_ratio`.
	MaxCardRatio float64

	// Maximum number of sorted dictionaries (based on cardinality/total_size and avg_data_length).
	MaxSortedDictionaries int
}

func NewUint8DefaultConfig() *Config {
	return &Config{
		Dictionaries: DictionariesConfig{
			StringColumns: DictionaryConfig{
				MinRowCount:           10,
				MaxCard:               math.MaxUint8,
				MaxCardRatio:          0.5,
				MaxSortedDictionaries: 5,
			},
			BinaryColumns: DictionaryConfig{
				MinRowCount:           10,
				MaxCard:               math.MaxUint8,
				MaxCardRatio:          0.5,
				MaxSortedDictionaries: 5,
			},
		},
	}
}

func NewUint16DefaultConfig() *Config {
	return &Config{
		Dictionaries: DictionariesConfig{
			StringColumns: DictionaryConfig{
				MinRowCount:           10,
				MaxCard:               math.MaxUint16,
				MaxCardRatio:          0.5,
				MaxSortedDictionaries: 5,
			},
			BinaryColumns: DictionaryConfig{
				MinRowCount:           10,
				MaxCard:               math.MaxUint16,
				MaxCardRatio:          0.5,
				MaxSortedDictionaries: 5,
			},
		},
	}
}

func NewConfigWithoutDictionary() *Config {
	return &Config{
		Dictionaries: DictionariesConfig{
			BinaryColumns: DictionaryConfig{
				MinRowCount: math.MaxInt,
				MaxCard:     0,
			},
			StringColumns: DictionaryConfig{
				MinRowCount: math.MaxInt,
				MaxCard:     0,
			},
		},
	}
}

func (c *Config) ConfigWithoutDictionarySupport() *Config {
	return NewConfigWithoutDictionary()
}

// IsDictionary returns true if the dictionary parameters passed in parameter satisfy the current
// dictionary configuration.
func (d *DictionaryConfig) IsDictionary(rowCount, card int, totalValueLength int) bool {
	return rowCount >= d.MinRowCount &&
		totalValueLength > 0 &&
		card <= d.MaxCard &&
		float64(card)/float64(rowCount) <= d.MaxCardRatio
}
