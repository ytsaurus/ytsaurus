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
			totalLengthStateName: flowtest.Schema("total_length:int64"),
		},
	})
}

func TestEventBecomesARequest(t *testing.T) {
	h := newKeeperHarness(t)

	r := h.Process(event(h, 7, "payload"))

	requests := r.MessagesOn(requestStreamID)
	require.Len(t, requests, 1)

	var request requestMessage
	require.NoError(t, requests[0].ConvertTo(&request))
	require.Equal(t, uint64(7), request.Key)
	require.Equal(t, "payload", request.Request)
	require.NotZero(t, request.RequestID)
}

func TestRequestsAreToldApart(t *testing.T) {
	h := newKeeperHarness(t)

	r := h.Process(event(h, 7, "first"), event(h, 7, "second"))

	requests := r.MessagesOn(requestStreamID)
	require.Len(t, requests, 2)
	require.NotEqual(t, requestIDOf(t, requests[0]), requestIDOf(t, requests[1]))
}

func TestTotalSurvivesTheBatch(t *testing.T) {
	h := newKeeperHarness(t)

	h.Process(response(h, 7, 5))
	r := h.Process(response(h, 7, 3))

	require.EqualValues(t, 8, totalOf(t, r, keyOf(h, 7)))
}

func TestKeysAccumulateApart(t *testing.T) {
	h := newKeeperHarness(t)

	r := h.Process(response(h, 7, 5), response(h, 9, 2), response(h, 7, 3))

	require.EqualValues(t, 8, totalOf(t, r, keyOf(h, 7)))
	require.EqualValues(t, 2, totalOf(t, r, keyOf(h, 9)))
}

func TestUnhandledStreamIsRefused(t *testing.T) {
	h := newKeeperHarness(t)

	err := h.ProcessError(h.KeyedMessage(requestStreamID, keyOf(h, 7), flowtest.Row{
		"request_id": uint64(1),
		"key":        uint64(7),
		"request":    "payload",
	}))

	require.ErrorContains(t, err, requestStreamID)
}

func keyOf(h *flowtest.Harness, key uint64) flow.Payload {
	return h.Key(flowtest.Row{"key": key})
}

func event(h *flowtest.Harness, key uint64, data string) flow.ExtendedMessage {
	return h.KeyedMessage(eventStreamID, keyOf(h, key), flowtest.Row{"key": key, "data": data})
}

func response(h *flowtest.Harness, key uint64, length int64) flow.ExtendedMessage {
	return h.KeyedMessage(responseStreamID, keyOf(h, key), flowtest.Row{
		"request_id": uint64(1),
		"key":        key,
		"length":     length,
	})
}

func requestIDOf(t *testing.T, msg flow.Message) uint64 {
	t.Helper()

	var request requestMessage
	require.NoError(t, msg.ConvertTo(&request))
	return request.RequestID
}

func totalOf(t *testing.T, r *flowtest.Response, key flow.Payload) int64 {
	t.Helper()

	row, ok := r.ExternalState(totalLengthStateName, key)
	require.True(t, ok, "no state stored for the key")

	var total totalLengthState
	require.NoError(t, row.ConvertTo(&total))
	return total.TotalLength
}
