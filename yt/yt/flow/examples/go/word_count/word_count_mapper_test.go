package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

// [BEGIN unit_test]

func newHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("mapper", &wordCountMapper{}), flowtest.Options{
		Streams:        map[string]flow.Schema{"words": flowtest.Schema("word:string")},
		KeySchema:      flowtest.Schema("word:string"),
		InternalStates: []string{wordStateName},
	})
}

func TestRepeatedWordAccumulates(t *testing.T) {
	h := newHarness(t)
	key := h.Key(flowtest.Row{"word": "hello"})

	var batch []flow.Input
	for range 3 {
		batch = append(batch, h.KeyedMessage("words", key, flowtest.Row{"word": "hello"}))
	}
	r := h.Process(batch...)

	require.EqualValues(t, 3, counterOf(t, r, key).Count)
}

func TestCounterSurvivesTheBatch(t *testing.T) {
	h := newHarness(t)
	key := h.Key(flowtest.Row{"word": "hello"})

	h.Process(h.KeyedMessage("words", key, flowtest.Row{"word": "hello"}))
	r := h.Process(h.KeyedMessage("words", key, flowtest.Row{"word": "hello"}))

	require.EqualValues(t, 2, counterOf(t, r, key).Count)
}

func TestWordsAreCountedApart(t *testing.T) {
	h := newHarness(t)
	hello := h.Key(flowtest.Row{"word": "hello"})
	world := h.Key(flowtest.Row{"word": "world"})

	r := h.Process(
		h.KeyedMessage("words", hello, flowtest.Row{"word": "hello"}),
		h.KeyedMessage("words", world, flowtest.Row{"word": "world"}),
		h.KeyedMessage("words", hello, flowtest.Row{"word": "hello"}),
	)

	require.EqualValues(t, 2, counterOf(t, r, hello).Count)
	require.EqualValues(t, 1, counterOf(t, r, world).Count)
}

func counterOf(t *testing.T, r *flowtest.Response, key flow.Payload) wordCountState {
	t.Helper()

	var counter wordCountState
	require.True(t, r.InternalStateYSON(wordStateName, key, &counter), "no counter stored for the key")
	return counter
}

// [END unit_test]
