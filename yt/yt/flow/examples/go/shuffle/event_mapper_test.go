package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

var eventSchema = flowtest.Schema(
	"value:string",
	"key_a:uint64",
	"key_b:uint64",
	"key_c:uint64",
	"key_d:uint64",
)

func newMapperHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowSourceComputation("reader", &eventMapper{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"queue": flowtest.Schema("data:string"),
			"event": eventSchema,
		},
	})
}

func queuedEvent(t *testing.T, h *flowtest.Harness, e event) flow.ExtendedMessage {
	t.Helper()

	data, err := json.Marshal(e)
	require.NoError(t, err)
	return h.Message("queue", flowtest.Row{"data": string(data)})
}

func TestJSONBecomesATypedEvent(t *testing.T) {
	h := newMapperHarness(t)

	r := h.Process(queuedEvent(t, h, event{Value: "v1", KeyA: 1, KeyB: 2, KeyC: 3, KeyD: 4}))

	require.Equal(t, []flowtest.Row{{
		"value": "v1",
		"key_a": uint64(1),
		"key_b": uint64(2),
		"key_c": uint64(3),
		"key_d": uint64(4),
	}}, r.Rows())
}

func TestUnparsableDataFailsTheBatch(t *testing.T) {
	h := newMapperHarness(t)

	err := h.ProcessError(h.Message("queue", flowtest.Row{"data": "}not json{"}))

	require.ErrorContains(t, err, "parsing the data column")
}
