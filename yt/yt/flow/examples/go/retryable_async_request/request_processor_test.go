package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

const (
	answeredAtOnce        uint64 = 9
	answeredOnRetry       uint64 = 11
	answeredOnSecondRetry uint64 = 10

	eventKey uint64 = 777
)

func newProcessorHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("processor", &requestProcessor{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			requestStreamID:  flowtest.Schema("request_id:uint64", "key:uint64", "request:string"),
			responseStreamID: flowtest.Schema("request_id:uint64", "key:uint64", "length:int64"),
		},
		KeySchema:      flowtest.Schema("request_id:uint64"),
		InternalStates: []string{requestStateName},
	})
}

func TestRequestAnsweredAtOnceLeavesNothingPending(t *testing.T) {
	h := newProcessorHarness(t)
	key := h.Key(flowtest.Row{"request_id": answeredAtOnce})

	r := h.Process(h.KeyedMessage(requestStreamID, key, requestRow(answeredAtOnce, "hello")))

	require.Equal(t, []flowtest.Row{{
		"request_id": answeredAtOnce,
		"key":        eventKey,
		"length":     int64(len("hello")),
	}}, r.Rows())
	require.Empty(t, r.Timers())
	require.False(t, r.InternalStateYSON(requestStateName, key, &requestState{}), "a request was left pending")
}

func TestRequestIsRetriedUntilItIsAnswered(t *testing.T) {
	h := newProcessorHarness(t)
	key := h.Key(flowtest.Row{"request_id": answeredOnSecondRetry})

	r := h.Process(h.KeyedMessage(requestStreamID, key, requestRow(answeredOnSecondRetry, "hello")))
	require.Empty(t, r.Messages())
	require.EqualValues(t, 1, pendingRequest(t, r, key).FailedAttempts)

	r = h.Process(h.Timer(key, 0))
	require.Empty(t, r.Messages())
	require.EqualValues(t, 2, pendingRequest(t, r, key).FailedAttempts)

	r = h.Process(h.Timer(key, 0))
	require.Len(t, r.Messages(), 1)
	require.Empty(t, r.Timers())
	require.True(t, r.InternalStateReset(requestStateName, key))
}

func TestTimerWithoutAStoredRequestDoesNothing(t *testing.T) {
	h := newProcessorHarness(t)
	key := h.Key(flowtest.Row{"request_id": answeredOnRetry})

	r := h.Process(h.Timer(key, 0))

	require.Empty(t, r.Messages())
	require.Empty(t, r.Timers())
	require.False(t, r.InternalStateWritten(requestStateName))
}

func TestRequestsAreRetriedApart(t *testing.T) {
	h := newProcessorHarness(t)
	answered := h.Key(flowtest.Row{"request_id": answeredAtOnce})
	retried := h.Key(flowtest.Row{"request_id": answeredOnRetry})

	r := h.Process(
		h.KeyedMessage(requestStreamID, answered, requestRow(answeredAtOnce, "hello")),
		h.KeyedMessage(requestStreamID, retried, requestRow(answeredOnRetry, "world")),
	)

	require.Len(t, r.Messages(), 1)
	require.Len(t, r.Timers(), 1)
	require.EqualValues(t, 1, pendingRequest(t, r, retried).FailedAttempts)
}

func requestRow(requestID uint64, request string) flowtest.Row {
	return flowtest.Row{"request_id": requestID, "key": eventKey, "request": request}
}

func pendingRequest(t *testing.T, r *flowtest.Response, key flow.Payload) requestState {
	t.Helper()

	var request requestState
	require.True(t, r.InternalStateYSON(requestStateName, key, &request), "no request stored for the key")
	return request
}
