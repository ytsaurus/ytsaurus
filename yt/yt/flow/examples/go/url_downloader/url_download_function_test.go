package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func newHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("url_downloader", &urlDownloadFunction{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"urls":         flowtest.Schema("host:string", "url:string"),
			outputStreamID: flowtest.Schema("host:string", "url:string", "data:string"),
		},
		KeySchema:      flowtest.Schema("host:string"),
		InternalStates: []string{hostStateName},
	})
}

func TestMessageJoinsTheBatchInsteadOfBeingProcessed(t *testing.T) {
	h := newHarness(t)
	key := h.Key(flowtest.Row{"host": "host_0"})

	r := h.Process(h.KeyedMessage("urls", key, flowtest.Row{"host": "host_0", "url": "host_0/item_0"}))

	require.Equal(t, hostState{Host: "host_0", PendingURLs: []string{"host_0/item_0"}}, batchOf(t, r, key))
	require.Empty(t, r.Messages())
	require.Len(t, r.Timers(), 1)
}

func TestTimerProcessesTheWholeBatchAtOnce(t *testing.T) {
	h := newHarness(t)
	key := h.Key(flowtest.Row{"host": "host_0"})

	h.Process(
		h.KeyedMessage("urls", key, flowtest.Row{"host": "host_0", "url": "host_0/item_0"}),
		h.KeyedMessage("urls", key, flowtest.Row{"host": "host_0", "url": "host_0/item_11"}),
	)
	r := h.Process(h.Timer(key, 0))

	require.Equal(t, []flowtest.Row{
		{"host": "host_0", "url": "host_0/item_0", "data": "length: 13, digits: 2"},
		{"host": "host_0", "url": "host_0/item_11", "data": "length: 14, digits: 3"},
	}, r.Rows())
	require.True(t, r.InternalStateReset(hostStateName, key))
}

func TestHostsAreBatchedApart(t *testing.T) {
	h := newHarness(t)
	first := h.Key(flowtest.Row{"host": "host_0"})
	second := h.Key(flowtest.Row{"host": "host_1"})

	h.Process(
		h.KeyedMessage("urls", first, flowtest.Row{"host": "host_0", "url": "host_0/item_0"}),
		h.KeyedMessage("urls", second, flowtest.Row{"host": "host_1", "url": "host_1/item_0"}),
	)

	require.Equal(t, []flowtest.Row{
		{"host": "host_0", "url": "host_0/item_0", "data": "length: 13, digits: 2"},
	}, h.Process(h.Timer(first, 0)).Rows())
	require.Equal(t, []flowtest.Row{
		{"host": "host_1", "url": "host_1/item_0", "data": "length: 13, digits: 2"},
	}, h.Process(h.Timer(second, 0)).Rows())
}

func TestTimerWithoutABatchProcessesNothing(t *testing.T) {
	h := newHarness(t)
	key := h.Key(flowtest.Row{"host": "host_0"})

	r := h.Process(h.Timer(key, 0))

	require.Empty(t, r.Messages())
}

func batchOf(t *testing.T, r *flowtest.Response, key flow.Payload) hostState {
	t.Helper()

	var batch hostState
	require.True(t, r.InternalStateYSON(hostStateName, key, &batch), "no batch stored for the key")
	return batch
}
