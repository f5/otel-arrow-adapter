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

// Infrastructure to manage related records.

import (
	"math"

	cfg "github.com/f5/otel-arrow-adapter/pkg/config"
	carrow "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/stats"
	"github.com/f5/otel-arrow-adapter/pkg/record_message"
)

type (
	// RelatedData is a collection of related/dependent data to log record
	// entities.
	RelatedData struct {
		logRecordCount uint64

		relatedRecordsManager *carrow.RelatedRecordsManager

		attrsBuilders *AttrsBuilders
	}

	// AttrsBuilders groups together AttrsBuilder instances used to build related
	// data attributes (i.e. resource attributes, scope attributes, and log record
	// attributes.
	AttrsBuilders struct {
		resource  *carrow.Attrs16Builder
		scope     *carrow.Attrs16Builder
		logRecord *carrow.Attrs16Builder
	}
)

func NewRelatedData(cfg *cfg.Config, stats *stats.ProducerStats) (*RelatedData, error) {
	rrManager := carrow.NewRelatedRecordsManager(cfg, stats)

	attrsResourceBuilder := rrManager.Declare(carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16Builder(b, carrow.PayloadTypes.ResourceAttrs)
	})

	attrsScopeBuilder := rrManager.Declare(carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16Builder(b, carrow.PayloadTypes.ScopeAttrs)
	})

	attrsLogRecordBuilder := rrManager.Declare(carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16Builder(b, carrow.PayloadTypes.LogRecordAttrs)
	})

	return &RelatedData{
		relatedRecordsManager: rrManager,
		attrsBuilders: &AttrsBuilders{
			resource:  attrsResourceBuilder.(*carrow.Attrs16Builder),
			scope:     attrsScopeBuilder.(*carrow.Attrs16Builder),
			logRecord: attrsLogRecordBuilder.(*carrow.Attrs16Builder),
		},
	}, nil
}

func (r *RelatedData) Release() {
	r.relatedRecordsManager.Release()
}

func (r *RelatedData) AttrsBuilders() *AttrsBuilders {
	return r.attrsBuilders
}

func (r *RelatedData) RecordBuilderExt(payloadType *carrow.PayloadType) *builder.RecordBuilderExt {
	return r.relatedRecordsManager.RecordBuilderExt(payloadType)
}

func (r *RelatedData) Reset() {
	r.logRecordCount = 0
	r.relatedRecordsManager.Reset()
}

func (r *RelatedData) LogRecordCount() uint16 {
	return uint16(r.logRecordCount)
}

func (r *RelatedData) NextSpanID() uint16 {
	c := r.logRecordCount

	if c == math.MaxUint16 {
		panic("maximum number of log records reached per batch, please reduce the batch size to a maximum of 65535 log records")
	}

	r.logRecordCount++
	return uint16(c)
}

func (r *RelatedData) BuildRecordMessages() ([]*record_message.RecordMessage, error) {
	return r.relatedRecordsManager.BuildRecordMessages()
}

func (ab *AttrsBuilders) Resource() *carrow.Attrs16Builder {
	return ab.resource
}

func (ab *AttrsBuilders) Scope() *carrow.Attrs16Builder {
	return ab.scope
}

func (ab *AttrsBuilders) LogRecord() *carrow.Attrs16Builder {
	return ab.logRecord
}
