package main

import (
	"context"

	"go.ytsaurus.tech/yt/go/flow"
)

const (
	shuffleStateName = "/shuffle-state"
	countColumn      = "count"
)

// [BEGIN event_reducer]

type shuffleState struct {
	Count int64 `yson:"count"`
}

type eventReducer struct{}

var _ flow.RowFunction = (*eventReducer)(nil)

func (*eventReducer) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	state, err := flow.OpenExternalState(rt, shuffleStateName, msg)
	if err != nil {
		return err
	}

	var counter shuffleState
	if _, err := state.ConvertTo(&counter); err != nil {
		return err
	}
	counter.Count++
	return state.ConvertFrom(&counter)
}

// [END event_reducer]
