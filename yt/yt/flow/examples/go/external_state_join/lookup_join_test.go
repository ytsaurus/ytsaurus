package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func newHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("lookup_join", &lookupJoin{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"event":          flowtest.Schema("key:uint64"),
			enrichedStreamID: flowtest.Schema("key:uint64", "name:string"),
		},
		KeySchema: flowtest.Schema("hash:uint64", "key:uint64"),
		JoinedExternalStates: map[string]flow.Schema{
			referenceStateName: flowtest.Schema("hash:uint64", "key:uint64", "name:string"),
		},
	})
}

func keyOf(h *flowtest.Harness, key uint64) flow.Payload {
	return h.Key(flowtest.Row{"hash": key, "key": key})
}

func TestKnownKeyIsEnriched(t *testing.T) {
	h := newHarness(t)
	key := keyOf(h, 1)
	h.PutJoinedExternalState(referenceStateName, key, flowtest.Row{"key": uint64(1), "name": "alice"})

	r := h.Process(h.KeyedMessage("event", key, flowtest.Row{"key": uint64(1)}))

	require.Equal(t, []flowtest.Row{{"key": uint64(1), "name": "alice"}}, r.Rows())
	require.Len(t, r.MessagesOn(enrichedStreamID), 1)
}

func TestUnknownKeyIsDropped(t *testing.T) {
	h := newHarness(t)

	r := h.Process(h.KeyedMessage("event", keyOf(h, 7), flowtest.Row{"key": uint64(7)}))

	require.Empty(t, r.Messages())
}

func TestReferenceRowWithoutANameIsDropped(t *testing.T) {
	h := newHarness(t)
	key := keyOf(h, 1)
	h.PutJoinedExternalState(referenceStateName, key, flowtest.Row{"key": uint64(1)})

	r := h.Process(h.KeyedMessage("event", key, flowtest.Row{"key": uint64(1)}))

	require.Empty(t, r.Messages())
}
