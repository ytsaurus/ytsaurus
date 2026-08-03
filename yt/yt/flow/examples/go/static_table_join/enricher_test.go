package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func newEnricherHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("enricher", &enricher{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"event":          flowtest.Schema("key:uint64"),
			enrichedStreamID: flowtest.Schema("key:uint64", "name:string"),
		},
		KeySchema:            joinKeySchema,
		JoinedExternalStates: map[string]flow.Schema{referenceStateName: referenceStateSchema},
	})
}

func TestEventIsEnrichedWithTheJoinedName(t *testing.T) {
	h := newEnricherHarness(t)
	key := h.Key(flowtest.Row{"key": uint64(1)})
	h.PutJoinedExternalState(referenceStateName, key, flowtest.Row{"normalized_name": "alice"})

	r := h.Process(h.KeyedMessage("event", key, flowtest.Row{"key": uint64(1)}))

	require.Equal(t, []flowtest.Row{{"key": uint64(1), "name": "alice"}}, r.Rows())
	require.Len(t, r.MessagesOn(enrichedStreamID), 1)
}

func TestEventWithoutAnyReferenceIsDropped(t *testing.T) {
	h := newEnricherHarness(t)
	key := h.Key(flowtest.Row{"key": uint64(1)})

	r := h.Process(h.KeyedMessage("event", key, flowtest.Row{"key": uint64(1)}))

	require.Empty(t, r.Messages())
}

func TestReferenceRowWithoutANameIsDropped(t *testing.T) {
	h := newEnricherHarness(t)
	key := h.Key(flowtest.Row{"key": uint64(1)})
	h.PutJoinedExternalState(referenceStateName, key, flowtest.Row{})

	r := h.Process(h.KeyedMessage("event", key, flowtest.Row{"key": uint64(1)}))

	require.Empty(t, r.Messages())
}
