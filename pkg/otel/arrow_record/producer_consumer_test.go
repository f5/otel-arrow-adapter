package arrow_record

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	arrowpb "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
	"github.com/f5/otel-arrow-adapter/pkg/datagen"
	"github.com/f5/otel-arrow-adapter/pkg/otel/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/protobuf/proto"
)

func FuzzConsumerTraces(f *testing.F) {
	const numSeeds = 5

	ent := datagen.NewTestEntropy(12345)

	for i := 0; i < numSeeds; i++ {
		dg := datagen.NewTracesGenerator(
			ent,
			ent.NewStandardResourceAttributes(),
			ent.NewStandardInstrumentationScopes(),
		)
		traces1 := dg.Generate(i+1, time.Minute)
		traces2 := dg.Generate(i+1, time.Minute)

		producer := NewProducer()

		batch1, err1 := producer.BatchArrowRecordsFromTraces(traces1)
		require.NoError(f, err1)
		batch2, err2 := producer.BatchArrowRecordsFromTraces(traces2)
		require.NoError(f, err2)

		b1b, err1 := proto.Marshal(batch1)
		b2b, err2 := proto.Marshal(batch2)

		f.Add(b1b, b2b)
	}

	f.Fuzz(func(t *testing.T, b1, b2 []byte) {
		var b1b arrowpb.BatchArrowRecords
		var b2b arrowpb.BatchArrowRecords

		if err := proto.Unmarshal(b1, &b1b); err != nil {
			return
		}
		if err := proto.Unmarshal(b2, &b1b); err != nil {
			return
		}

		consumer := NewConsumer()

		if _, err := consumer.TracesFrom(&b1b); err != nil {
			return
		}
		if _, err := consumer.TracesFrom(&b2b); err != nil {
			return
		}
	})
}

func FuzzProducerTraces2(f *testing.F) {
	const numSeeds = 5

	ent := datagen.NewTestEntropy(12345)

	for i := 0; i < numSeeds; i++ {
		dg := datagen.NewTracesGenerator(
			ent,
			ent.NewStandardResourceAttributes(),
			ent.NewStandardInstrumentationScopes(),
		)
		traces1 := dg.Generate(i+1, time.Minute)
		traces2 := dg.Generate(i+1, time.Minute)

		b1b, err1 := ptraceotlp.NewExportRequestFromTraces(traces1).MarshalProto()
		if err1 != nil {
			panic(err1)
		}
		b2b, err2 := ptraceotlp.NewExportRequestFromTraces(traces2).MarshalProto()
		if err2 != nil {
			panic(err2)
		}

		f.Add(b1b, b2b)
	}

	f.Fuzz(func(t *testing.T, b1, b2 []byte) {
		e1 := ptraceotlp.NewExportRequest()
		e2 := ptraceotlp.NewExportRequest()

		if err := e1.UnmarshalProto(b1); err != nil {
			return
		}
		if err := e2.UnmarshalProto(b2); err != nil {
			return
		}

		producer := NewProducer()

		if _, err := producer.BatchArrowRecordsFromTraces(e1.Traces()); err != nil {
			return
		}
		if _, err := producer.BatchArrowRecordsFromTraces(e2.Traces()); err != nil {
			return
		}
	})
}

func FuzzProducerTraces1(f *testing.F) {
	const numSeeds = 5

	ent := datagen.NewTestEntropy(12345)

	dg := datagen.NewTracesGenerator(
		ent,
		ent.NewStandardResourceAttributes(),
		ent.NewStandardInstrumentationScopes(),
	)
	traces1 := dg.Generate(1, time.Minute)

	for i := 0; i < numSeeds; i++ {
		traces2 := dg.Generate(11, time.Minute)

		b2b, err2 := ptraceotlp.NewExportRequestFromTraces(traces2).MarshalProto()
		if err2 != nil {
			panic(err2)
		}

		f.Add(b2b)
	}

	f.Fuzz(func(t *testing.T, b2 []byte) {
		e2 := ptraceotlp.NewExportRequest()

		if err := e2.UnmarshalProto(b2); err != nil {
			return
		}

		producer := NewProducer()

		if _, err := producer.BatchArrowRecordsFromTraces(traces1); err != nil {
			t.Error("unexpected fail", err)
		}
		if _, err := producer.BatchArrowRecordsFromTraces(e2.Traces()); err != nil {
			return
		}
	})
}

func TestProducerConsumerTraces(t *testing.T) {
	ent := datagen.NewTestEntropy(int64(rand.Uint64()))

	dg := datagen.NewTracesGenerator(
		ent,
		ent.NewStandardResourceAttributes(),
		ent.NewStandardInstrumentationScopes(),
	)
	traces := dg.Generate(10, time.Minute)

	producer := NewProducer()

	batch, err := producer.BatchArrowRecordsFromTraces(traces)
	require.NoError(t, err)
	require.Equal(t, arrowpb.OtlpArrowPayloadType_SPANS, batch.OtlpArrowPayloads[0].Type)

	consumer := NewConsumer()
	received, err := consumer.TracesFrom(batch)
	require.Equal(t, 1, len(received))

	assert.Equiv(
		t,
		[]json.Marshaler{ptraceotlp.NewExportRequestFromTraces(traces)},
		[]json.Marshaler{ptraceotlp.NewExportRequestFromTraces(received[0])},
	)
}

func TestProducerConsumerLogs(t *testing.T) {
	ent := datagen.NewTestEntropy(int64(rand.Uint64()))

	dg := datagen.NewLogsGenerator(
		ent,
		ent.NewStandardResourceAttributes(),
		ent.NewStandardInstrumentationScopes(),
	)
	logs := dg.Generate(10, time.Minute)

	producer := NewProducer()

	batch, err := producer.BatchArrowRecordsFromLogs(logs)
	require.NoError(t, err)
	require.Equal(t, arrowpb.OtlpArrowPayloadType_LOGS, batch.OtlpArrowPayloads[0].Type)

	consumer := NewConsumer()
	received, err := consumer.LogsFrom(batch)
	require.Equal(t, 1, len(received))

	assert.Equiv(
		t,
		[]json.Marshaler{plogotlp.NewExportRequestFromLogs(logs)},
		[]json.Marshaler{plogotlp.NewExportRequestFromLogs(received[0])},
	)
}
