package arrow_test

import (
	"testing"

	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/f5/otel-arrow-adapter/pkg/otel/traces/arrow"
)

func TestStatus(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()
	sb := arrow.NewStatusBuilder(pool)

	sb.Append(Status1())
	sb.Append(Status2())
	arr := sb.Build()
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"code":1,"status_message":"message1"}
,{"code":2,"status_message":"message2"}
]`

	require.JSONEq(t, expected, string(json))
}

func TestEvent(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()
	eb := arrow.NewEventBuilder(pool)

	eb.Append(Event1())
	eb.Append(Event2())
	arr := eb.Build()
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1}
,{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"name":"event2","time_unix_nano":2}
]`

	require.JSONEq(t, expected, string(json))
}

func TestLink(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()
	lb := arrow.NewLinkBuilder(pool)

	lb.Append(Link1())
	lb.Append(Link2())
	arr := lb.Build()
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value1"}
,{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}
]`

	require.JSONEq(t, expected, string(json))
}

func TestSpan(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()
	sb := arrow.NewSpanBuilder(pool)

	sb.Append(Span1())
	sb.Append(Span2())
	arr := sb.Build()
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]}],"dropped_attributes_count":0,"dropped_events_count":0,"dropped_links_count":0,"end_time_unix_nano":2,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"name":"event2","time_unix_nano":2}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value1"},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span1","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":1,"status":{"code":1,"status_message":"message1"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value1"}
,{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"dropped_events_count":1,"dropped_links_count":1,"end_time_unix_nano":4,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span2","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":3,"status":{"code":2,"status_message":"message2"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value2"}
]`

	require.JSONEq(t, expected, string(json))
}

func TestScopeSpans(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()
	ssb := arrow.NewScopeSpansBuilder(pool)

	ssb.Append(ScopeSpans1())
	ssb.Append(ScopeSpans2())
	arr := ssb.Build()
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"schema_url":"schema1","scope":{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"dropped_attributes_count":0,"name":"scope1","version":"1.0.1"},"spans":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]}],"dropped_attributes_count":0,"dropped_events_count":0,"dropped_links_count":0,"end_time_unix_nano":2,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"name":"event2","time_unix_nano":2}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value1"},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span1","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":1,"status":{"code":1,"status_message":"message1"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value1"},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"dropped_events_count":1,"dropped_links_count":1,"end_time_unix_nano":4,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span2","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":3,"status":{"code":2,"status_message":"message2"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value2"}]}
,{"schema_url":"schema2","scope":{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"dropped_attributes_count":1,"name":"scope2","version":"1.0.2"},"spans":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"dropped_events_count":1,"dropped_links_count":1,"end_time_unix_nano":4,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span2","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":3,"status":{"code":2,"status_message":"message2"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value2"}]}
]`

	require.JSONEq(t, expected, string(json))
}

func TestResourceSpans(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()
	rsb := arrow.NewResourceSpansBuilder(pool)

	rsb.Append(ResourceSpans1())
	rsb.Append(ResourceSpans2())
	arr := rsb.Build()
	defer arr.Release()

	json, err := arr.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"resource":{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"dropped_attributes_count":0},"schema_url":"schema1","scope_spans":[{"schema_url":"schema1","scope":{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"dropped_attributes_count":0,"name":"scope1","version":"1.0.1"},"spans":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]}],"dropped_attributes_count":0,"dropped_events_count":0,"dropped_links_count":0,"end_time_unix_nano":2,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"name":"event2","time_unix_nano":2}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value1"},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span1","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":1,"status":{"code":1,"status_message":"message1"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value1"},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"dropped_events_count":1,"dropped_links_count":1,"end_time_unix_nano":4,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span2","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":3,"status":{"code":2,"status_message":"message2"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value2"}]},{"schema_url":"schema2","scope":{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"dropped_attributes_count":1,"name":"scope2","version":"1.0.2"},"spans":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"dropped_events_count":1,"dropped_links_count":1,"end_time_unix_nano":4,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span2","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":3,"status":{"code":2,"status_message":"message2"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value2"}]}]}
,{"resource":{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"dropped_attributes_count":1},"schema_url":"schema2","scope_spans":[{"schema_url":"schema2","scope":{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"dropped_attributes_count":1,"name":"scope2","version":"1.0.2"},"spans":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"dropped_events_count":1,"dropped_links_count":1,"end_time_unix_nano":4,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span2","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":3,"status":{"code":2,"status_message":"message2"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value2"}]}]}
]`

	require.JSONEq(t, expected, string(json))
}

func TestTraces(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()
	tb := arrow.NewTracesBuilder(pool)

	tb.Append(Traces())
	record := tb.Build()
	defer record.Release()

	json, err := record.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	expected := `[{"resource_spans":[{"resource":{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"dropped_attributes_count":0},"schema_url":"schema1","scope_spans":[{"schema_url":"schema1","scope":{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]},{"key":"bytes","value":[4,"Ynl0ZXMx"]}],"dropped_attributes_count":0,"name":"scope1","version":"1.0.1"},"spans":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]}],"dropped_attributes_count":0,"dropped_events_count":0,"dropped_links_count":0,"end_time_unix_nano":2,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"name":"event2","time_unix_nano":2}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value1"},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span1","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":1,"status":{"code":1,"status_message":"message1"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value1"},{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"dropped_events_count":1,"dropped_links_count":1,"end_time_unix_nano":4,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span2","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":3,"status":{"code":2,"status_message":"message2"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value2"}]},{"schema_url":"schema2","scope":{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"dropped_attributes_count":1,"name":"scope2","version":"1.0.2"},"spans":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"dropped_events_count":1,"dropped_links_count":1,"end_time_unix_nano":4,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span2","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":3,"status":{"code":2,"status_message":"message2"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value2"}]}]},{"resource":{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"dropped_attributes_count":1},"schema_url":"schema2","scope_spans":[{"schema_url":"schema2","scope":{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bytes","value":[4,"Ynl0ZXMy"]}],"dropped_attributes_count":1,"name":"scope2","version":"1.0.2"},"spans":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]}],"dropped_attributes_count":1,"dropped_events_count":1,"dropped_links_count":1,"end_time_unix_nano":4,"events":[{"attributes":[{"key":"str","value":[0,"string1"]},{"key":"int","value":[1,1]},{"key":"double","value":[2,1]},{"key":"bool","value":[3,true]}],"dropped_attributes_count":0,"name":"event1","time_unix_nano":1}],"kind":3,"links":[{"attributes":[{"key":"str","value":[0,"string2"]},{"key":"int","value":[1,2]},{"key":"double","value":[2,2]},{"key":"bool","value":[3,false]}],"dropped_attributes_count":1,"span_id":"qgAAAAAAAAA=","trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key2=value2"}],"name":"span2","parent_span_id":"qgAAAAAAAAA=","span_id":"qgAAAAAAAAA=","start_time_unix_nano":3,"status":{"code":2,"status_message":"message2"},"trace_id":"qgAAAAAAAAAAAAAAAAAAAA==","trace_state":"key1=value2"}]}]}]}
]`

	require.JSONEq(t, expected, string(json))
}

func Status1() ptrace.SpanStatus {
	status := ptrace.NewSpanStatus()
	status.SetCode(ptrace.StatusCodeOk)
	status.SetMessage("message1")
	return status
}

func Status2() ptrace.SpanStatus {
	status := ptrace.NewSpanStatus()
	status.SetCode(ptrace.StatusCodeError)
	status.SetMessage("message2")
	return status
}

func Event1() ptrace.SpanEvent {
	event := ptrace.NewSpanEvent()
	event.SetName("event1")
	event.SetTimestamp(1)
	attrs := event.Attributes()
	attrs.PutStr("str", "string1")
	attrs.PutInt("int", 1)
	attrs.PutDouble("double", 1)
	attrs.PutBool("bool", true)
	event.SetDroppedAttributesCount(0)
	return event
}

func Event2() ptrace.SpanEvent {
	event := ptrace.NewSpanEvent()
	event.SetName("event2")
	event.SetTimestamp(2)
	attrs := event.Attributes()
	attrs.PutStr("str", "string2")
	attrs.PutInt("int", 2)
	attrs.PutDouble("double", 2)
	event.SetDroppedAttributesCount(1)
	return event
}

func Link1() ptrace.SpanLink {
	link := ptrace.NewSpanLink()
	link.SetTraceID([16]byte{0xAA})
	link.SetSpanID([8]byte{0xAA})
	link.TraceState().FromRaw("key1=value1")
	attrs := link.Attributes()
	attrs.PutStr("str", "string1")
	attrs.PutInt("int", 1)
	attrs.PutDouble("double", 1)
	attrs.PutBool("bool", true)
	link.SetDroppedAttributesCount(0)
	return link
}

func Link2() ptrace.SpanLink {
	link := ptrace.NewSpanLink()
	link.SetTraceID([16]byte{0xAA})
	link.SetSpanID([8]byte{0xAA})
	link.TraceState().FromRaw("key2=value2")
	attrs := link.Attributes()
	attrs.PutStr("str", "string2")
	attrs.PutInt("int", 2)
	attrs.PutDouble("double", 2)
	attrs.PutBool("bool", false)
	link.SetDroppedAttributesCount(1)
	return link
}

func Span1() ptrace.Span {
	span := ptrace.NewSpan()
	span.SetStartTimestamp(1)
	span.SetEndTimestamp(2)
	span.SetTraceID([16]byte{0xAA})
	span.SetSpanID([8]byte{0xAA})
	span.TraceState().FromRaw("key1=value1")
	span.SetParentSpanID([8]byte{0xAA})
	span.SetName("span1")
	span.SetKind(ptrace.SpanKindClient)
	attrs := span.Attributes()
	attrs.PutStr("str", "string1")
	attrs.PutInt("int", 1)
	attrs.PutDouble("double", 1)
	span.SetDroppedAttributesCount(0)
	events := span.Events()
	evt := events.AppendEmpty()
	Event1().CopyTo(evt)
	evt = events.AppendEmpty()
	Event2().CopyTo(evt)
	span.SetDroppedEventsCount(0)
	links := span.Links()
	lnk := links.AppendEmpty()
	Link1().CopyTo(lnk)
	lnk = links.AppendEmpty()
	Link2().CopyTo(lnk)
	span.SetDroppedLinksCount(0)
	status := span.Status()
	Status1().CopyTo(status)
	return span
}

func Span2() ptrace.Span {
	span := ptrace.NewSpan()
	span.SetStartTimestamp(3)
	span.SetEndTimestamp(4)
	span.SetTraceID([16]byte{0xAA})
	span.SetSpanID([8]byte{0xAA})
	span.TraceState().FromRaw("key1=value2")
	span.SetParentSpanID([8]byte{0xAA})
	span.SetName("span2")
	span.SetKind(ptrace.SpanKindClient)
	attrs := span.Attributes()
	attrs.PutStr("str", "string2")
	attrs.PutInt("int", 2)
	attrs.PutDouble("double", 2)
	span.SetDroppedAttributesCount(1)
	events := span.Events()
	evt := events.AppendEmpty()
	Event1().CopyTo(evt)
	span.SetDroppedEventsCount(1)
	links := span.Links()
	lnk := links.AppendEmpty()
	Link2().CopyTo(lnk)
	span.SetDroppedLinksCount(1)
	status := span.Status()
	Status2().CopyTo(status)
	return span
}

func Attrs1() pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutStr("str", "string1")
	attrs.PutInt("int", 1)
	attrs.PutDouble("double", 1.0)
	attrs.PutBool("bool", true)
	bytes := attrs.PutEmptyBytes("bytes")
	bytes.Append([]byte("bytes1")...)
	return attrs
}

func Attrs2() pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutStr("str", "string2")
	attrs.PutInt("int", 2)
	attrs.PutDouble("double", 2.0)
	bytes := attrs.PutEmptyBytes("bytes")
	bytes.Append([]byte("bytes2")...)
	return attrs
}

func Scope1() pcommon.InstrumentationScope {
	scope := pcommon.NewInstrumentationScope()
	scope.SetName("scope1")
	scope.SetVersion("1.0.1")
	scopeAttrs := scope.Attributes()
	Attrs1().CopyTo(scopeAttrs)
	scope.SetDroppedAttributesCount(0)
	return scope
}

func Scope2() pcommon.InstrumentationScope {
	scope := pcommon.NewInstrumentationScope()
	scope.SetName("scope2")
	scope.SetVersion("1.0.2")
	scopeAttrs := scope.Attributes()
	Attrs2().CopyTo(scopeAttrs)
	scope.SetDroppedAttributesCount(1)
	return scope
}

func Resource1() pcommon.Resource {
	resource := pcommon.NewResource()
	resourceAttrs := resource.Attributes()
	Attrs1().CopyTo(resourceAttrs)
	resource.SetDroppedAttributesCount(0)
	return resource
}

func Resource2() pcommon.Resource {
	resource := pcommon.NewResource()
	resourceAttrs := resource.Attributes()
	Attrs2().CopyTo(resourceAttrs)
	resource.SetDroppedAttributesCount(1)
	return resource
}

func ScopeSpans1() ptrace.ScopeSpans {
	scopeSpans := ptrace.NewScopeSpans()
	scope := scopeSpans.Scope()
	Scope1().CopyTo(scope)
	scopeSpans.SetSchemaUrl("schema1")
	spans := scopeSpans.Spans()
	span := spans.AppendEmpty()
	Span1().CopyTo(span)
	span = spans.AppendEmpty()
	Span2().CopyTo(span)
	return scopeSpans
}

func ScopeSpans2() ptrace.ScopeSpans {
	scopeSpans := ptrace.NewScopeSpans()
	scope := scopeSpans.Scope()
	Scope2().CopyTo(scope)
	scopeSpans.SetSchemaUrl("schema2")
	spans := scopeSpans.Spans()
	span := spans.AppendEmpty()
	Span2().CopyTo(span)
	return scopeSpans
}

func ResourceSpans1() ptrace.ResourceSpans {
	rs := ptrace.NewResourceSpans()
	resource := rs.Resource()
	Resource1().CopyTo(resource)
	scopeSpansSlice := rs.ScopeSpans()
	scopeSpans := scopeSpansSlice.AppendEmpty()
	ScopeSpans1().CopyTo(scopeSpans)
	scopeSpans = scopeSpansSlice.AppendEmpty()
	ScopeSpans2().CopyTo(scopeSpans)
	rs.SetSchemaUrl("schema1")
	return rs
}

func ResourceSpans2() ptrace.ResourceSpans {
	rs := ptrace.NewResourceSpans()
	resource := rs.Resource()
	Resource2().CopyTo(resource)
	scopeSpansSlice := rs.ScopeSpans()
	scopeSpans := scopeSpansSlice.AppendEmpty()
	ScopeSpans2().CopyTo(scopeSpans)
	rs.SetSchemaUrl("schema2")
	return rs
}

func Traces() ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpansSlice := traces.ResourceSpans()
	resourceSpans := resourceSpansSlice.AppendEmpty()
	ResourceSpans1().CopyTo(resourceSpans)
	resourceSpans = resourceSpansSlice.AppendEmpty()
	ResourceSpans2().CopyTo(resourceSpans)
	return traces
}
