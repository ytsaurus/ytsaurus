package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

var joinKeySchema = flowtest.Schema("hash:uint64", "key:uint64")

var referenceStateSchema = flowtest.Schema("hash:uint64", "key:uint64", "normalized_name:string")

func newLoaderHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("reference_loader", &referenceLoader{}), flowtest.Options{
		Streams:        map[string]flow.Schema{"reference": flowtest.Schema("key:uint64", "name:string")},
		KeySchema:      joinKeySchema,
		ExternalStates: map[string]flow.Schema{referenceStateName: referenceStateSchema},
	})
}

func TestReferenceNameIsNormalized(t *testing.T) {
	h := newLoaderHarness(t)
	key := h.Key(flowtest.Row{"key": uint64(1)})

	r := h.Process(h.KeyedMessage("reference", key, flowtest.Row{"key": uint64(1), "name": "  Alice "}))

	require.Equal(t, flowtest.Row{"normalized_name": "alice"}, r.ExternalStateRow(referenceStateName, key))
	require.Empty(t, r.Messages())
}

func TestRebuiltReferenceReplacesTheName(t *testing.T) {
	h := newLoaderHarness(t)
	key := h.Key(flowtest.Row{"key": uint64(1)})

	h.Process(h.KeyedMessage("reference", key, flowtest.Row{"key": uint64(1), "name": "Alice"}))
	r := h.Process(h.KeyedMessage("reference", key, flowtest.Row{"key": uint64(1), "name": "Bob"}))

	require.Equal(t, flowtest.Row{"normalized_name": "bob"}, r.ExternalStateRow(referenceStateName, key))
}
