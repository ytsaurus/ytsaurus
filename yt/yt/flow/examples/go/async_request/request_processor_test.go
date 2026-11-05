package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func newProcessorHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("processor", &requestProcessor{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			requestStreamID:  flowtest.Schema("request_id:uint64", "key:uint64", "request:string"),
			responseStreamID: flowtest.Schema("request_id:uint64", "key:uint64", "length:int64"),
		},
		KeySchema: flowtest.Schema("request_id:uint64"),
	})
}

func TestResponseAnswersTheRequest(t *testing.T) {
	h := newProcessorHarness(t)

	r := h.Process(request(h, 100, 7, "payload"))

	require.Equal(t, []flowtest.Row{{
		"request_id": uint64(100),
		"key":        uint64(7),
		"length":     int64(7),
	}}, r.Rows())
}

func request(h *flowtest.Harness, requestID, key uint64, payload string) flow.ExtendedMessage {
	return h.KeyedMessage(requestStreamID, h.Key(flowtest.Row{"request_id": requestID}), flowtest.Row{
		"request_id": requestID,
		"key":        key,
		"request":    payload,
	})
}
