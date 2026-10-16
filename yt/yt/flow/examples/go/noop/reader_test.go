package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func TestReaderEmitsNothing(t *testing.T) {
	h := flowtest.New(t, flow.NewRowSourceComputation("reader", &reader{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"random": flowtest.Schema("key:string", "data:string"),
		},
	})

	r := h.Process(h.Message("random", flowtest.Row{"key": "k", "data": "d"}))

	require.Empty(t, r.Rows())
}
