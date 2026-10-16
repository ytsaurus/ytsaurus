package main

import (
	"context"
	"time"

	"go.ytsaurus.tech/yt/go/flow"
)

const (
	requestStateName = "request-state"

	attemptsToSucceed = 3

	retryDelay = 5 * time.Second
)

// [BEGIN request_state]

type requestState struct {
	RequestID      uint64 `yson:"request_id"`
	Key            uint64 `yson:"key"`
	Request        string `yson:"request"`
	FailedAttempts int64  `yson:"failed_attempts"`
}

// [END request_state]

// [BEGIN request_processor]

type requestProcessor struct{}

var (
	_ flow.RowFunction      = (*requestProcessor)(nil)
	_ flow.RowTimerFunction = (*requestProcessor)(nil)
)

func (p *requestProcessor) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	state, err := flow.OpenYSONState[requestState](rt, requestStateName, msg)
	if err != nil {
		return err
	}

	var input requestMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}
	request := requestState{
		RequestID: input.RequestID,
		Key:       input.Key,
		Request:   input.Request,
	}
	return p.attempt(rt, state, request, out)
}

func (p *requestProcessor) OnTimer(
	ctx context.Context,
	rt flow.Runtime,
	timer flow.Timer,
	out flow.OutputCollector,
) error {
	state, err := flow.OpenYSONState[requestState](rt, requestStateName, timer)
	if err != nil {
		return err
	}

	if state.Empty() {
		return nil
	}
	return p.attempt(rt, state, *state.Value(), out)
}

func (p *requestProcessor) attempt(
	rt flow.Runtime,
	state *flow.YSONState[requestState],
	request requestState,
	out flow.OutputCollector,
) error {
	if !succeeds(request) {
		request.FailedAttempts++
		*state.Value() = request
		out.AddTimer(flow.TimerRequest{TriggerTimestamp: uint64(time.Now().Add(retryDelay).Unix())})
		return nil
	}

	response := flow.NewYSONMessage[responseMessage](responseStreamID)
	response.RequestID = request.RequestID
	response.Key = request.Key
	response.Length = int64(len(request.Request))
	encoded, err := flow.ConvertFrom(rt, response)
	if err != nil {
		return err
	}
	out.AddMessage(encoded)

	state.Clear()
	return nil
}

func succeeds(request requestState) bool {
	return (request.RequestID+uint64(request.FailedAttempts))%attemptsToSucceed == 0
}

// [END request_processor]
