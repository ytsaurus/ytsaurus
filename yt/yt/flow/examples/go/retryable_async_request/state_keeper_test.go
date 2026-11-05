package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func newKeeperHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("state", &stateKeeper{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			eventStreamID:    flowtest.Schema("key:uint64", "data:string"),
			requestStreamID:  flowtest.Schema("request_id:uint64", "key:uint64", "request:string"),
			responseStreamID: flowtest.Schema("request_id:uint64", "key:uint64", "length:int64"),
		},
		KeySchema: flowtest.Schema("key:uint64"),
		ExternalStates: map[string]flow.Schema{
			totalStateName: flowtest.Schema("key:uint64", "total_length:int64"),
		},
	})
}

func TestEventBecomesARequest(t *testing.T) {
	h := newKeeperHarness(t)
	key := h.Key(flowtest.Row{"key": eventKey})

	r := h.Process(h.KeyedMessage(eventStreamID, key, flowtest.Row{"key": eventKey, "data": "hello"}))

	rows := r.Rows()
	require.Len(t, rows, 1)
	require.Equal(t, eventKey, rows[0]["key"])
	require.Equal(t, "hello", rows[0]["request"])
	require.Contains(t, rows[0], "request_id")
	require.False(t, r.ExternalStateWritten(totalStateName))
}

func TestEventsOfOneKeyGetDistinctRequestIDs(t *testing.T) {
	h := newKeeperHarness(t)
	key := h.Key(flowtest.Row{"key": eventKey})

	r := h.Process(
		h.KeyedMessage(eventStreamID, key, flowtest.Row{"key": eventKey, "data": "hello"}),
		h.KeyedMessage(eventStreamID, key, flowtest.Row{"key": eventKey, "data": "world"}),
	)

	rows := r.Rows()
	require.Len(t, rows, 2)
	require.NotEqual(t, rows[0]["request_id"], rows[1]["request_id"])
}

func TestResponsesAccumulateAcrossBatches(t *testing.T) {
	h := newKeeperHarness(t)
	key := h.Key(flowtest.Row{"key": eventKey})

	h.Process(h.KeyedMessage(responseStreamID, key, responseRow(5)))
	r := h.Process(h.KeyedMessage(responseStreamID, key, responseRow(7)))

	require.EqualValues(t, 12, r.ExternalStateRow(totalStateName, key)["total_length"])
}

func TestTotalsAreKeptPerKey(t *testing.T) {
	h := newKeeperHarness(t)
	first := h.Key(flowtest.Row{"key": eventKey})
	second := h.Key(flowtest.Row{"key": eventKey + 1})

	r := h.Process(
		h.KeyedMessage(responseStreamID, first, responseRow(5)),
		h.KeyedMessage(responseStreamID, second, responseRow(7)),
		h.KeyedMessage(responseStreamID, first, responseRow(1)),
	)

	require.EqualValues(t, 6, r.ExternalStateRow(totalStateName, first)["total_length"])
	require.EqualValues(t, 7, r.ExternalStateRow(totalStateName, second)["total_length"])
}

func TestMessageOnAnUnhandledStreamFailsTheBatch(t *testing.T) {
	h := newKeeperHarness(t)
	key := h.Key(flowtest.Row{"key": eventKey})

	err := h.ProcessError(h.KeyedMessage(requestStreamID, key, requestRow(answeredAtOnce, "hello")))

	require.ErrorContains(t, err, `unhandled stream "request"`)
}

func responseRow(length int64) flowtest.Row {
	return flowtest.Row{"request_id": answeredAtOnce, "key": eventKey, "length": length}
}
