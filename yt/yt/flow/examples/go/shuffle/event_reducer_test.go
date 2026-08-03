package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

// [BEGIN reducer_unit_test]

var shuffleStreams = []string{"event_a", "event_b", "event_c", "event_d"}

func newReducerHarness(t *testing.T) *flowtest.Harness {
	streams := make(map[string]flow.Schema, len(shuffleStreams))
	for _, streamID := range shuffleStreams {
		streams[streamID] = eventSchema
	}

	return flowtest.New(t, flow.NewRowComputation("reducer", &eventReducer{}), flowtest.Options{
		Streams:        streams,
		KeySchema:      flowtest.Schema("value:string"),
		ExternalStates: map[string]flow.Schema{shuffleStateName: flowtest.Schema("count:int64")},
	})
}

func TestAValueIsCountedOncePerShuffleStream(t *testing.T) {
	h := newReducerHarness(t)
	key := h.Key(flowtest.Row{"value": "v"})

	var batch []flow.Input
	for _, streamID := range shuffleStreams {
		batch = append(batch, h.KeyedMessage(streamID, key, flowtest.Row{"value": "v"}))
	}
	r := h.Process(batch...)

	require.EqualValues(t, 4, countOf(t, r, key))
	require.Empty(t, r.Messages())
	require.Empty(t, r.Timers())
}

func TestValuesAreCountedApart(t *testing.T) {
	h := newReducerHarness(t)
	first := h.Key(flowtest.Row{"value": "v1"})
	second := h.Key(flowtest.Row{"value": "v2"})

	r := h.Process(
		h.KeyedMessage("event_a", first, flowtest.Row{"value": "v1"}),
		h.KeyedMessage("event_b", second, flowtest.Row{"value": "v2"}),
		h.KeyedMessage("event_c", first, flowtest.Row{"value": "v1"}),
	)

	require.EqualValues(t, 2, countOf(t, r, first))
	require.EqualValues(t, 1, countOf(t, r, second))
}

func TestCounterSurvivesTheBatch(t *testing.T) {
	h := newReducerHarness(t)
	key := h.Key(flowtest.Row{"value": "v"})

	h.Process(h.KeyedMessage("event_a", key, flowtest.Row{"value": "v"}))
	r := h.Process(h.KeyedMessage("event_b", key, flowtest.Row{"value": "v"}))

	require.EqualValues(t, 2, countOf(t, r, key))
}

func countOf(t *testing.T, r *flowtest.Response, key flow.Payload) int64 {
	t.Helper()

	row, ok := r.ExternalState(shuffleStateName, key)
	require.True(t, ok, "no counter stored for the key")

	count, err := row.Int64(countColumn)
	require.NoError(t, err)
	return count
}

// [END reducer_unit_test]
