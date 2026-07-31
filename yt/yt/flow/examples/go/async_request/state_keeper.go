package main

import (
	"context"
	"fmt"
	"math/rand/v2"

	"go.ytsaurus.tech/yt/go/flow"
)

const (
	eventStreamID    = "event"
	requestStreamID  = "request"
	responseStreamID = "response"
)

const totalLengthStateName = "/state"

// [BEGIN state_keeper]

type totalLengthState struct {
	TotalLength int64 `yson:"total_length"`
}

type stateKeeper struct{}

var _ flow.RowFunction = (*stateKeeper)(nil)

func (k *stateKeeper) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	switch msg.StreamID {
	case eventStreamID:
		return k.sendRequest(rt, msg, out)
	case responseStreamID:
		return k.accumulate(rt, msg)
	default:
		return fmt.Errorf("unhandled stream %q", msg.StreamID)
	}
}

func (*stateKeeper) sendRequest(rt flow.Runtime, msg flow.ExtendedMessage, out flow.OutputCollector) error {
	var event eventMessage
	if err := msg.ConvertTo(&event); err != nil {
		return err
	}

	request := flow.NewYSONMessage[requestMessage](requestStreamID)
	request.RequestID = rand.Uint64()
	request.Key = event.Key
	request.Request = event.Data

	encoded, err := flow.ConvertFrom(rt, request)
	if err != nil {
		return err
	}
	out.AddMessage(encoded)
	return nil
}

func (*stateKeeper) accumulate(rt flow.Runtime, msg flow.ExtendedMessage) error {
	var response responseMessage
	if err := msg.ConvertTo(&response); err != nil {
		return err
	}

	state, err := flow.OpenExternalState(rt, totalLengthStateName, msg)
	if err != nil {
		return err
	}

	var total totalLengthState
	if _, err := state.ConvertTo(&total); err != nil {
		return err
	}
	total.TotalLength += response.Length
	return state.ConvertFrom(&total)
}

// [END state_keeper]
