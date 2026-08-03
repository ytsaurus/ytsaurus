package main

import (
	"context"

	"go.ytsaurus.tech/yt/go/flow"
)

const wordStateName = "word-state"

// [BEGIN word_count_state]

type wordCountState struct {
	Word  string `yson:"word"`
	Count int64  `yson:"count"`
}

// [END word_count_state]

// [BEGIN word_count_mapper]

type wordCountMapper struct{}

var _ flow.RowFunction = (*wordCountMapper)(nil)

func (*wordCountMapper) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input wordMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	state, err := flow.OpenYSONState[wordCountState](rt, wordStateName, msg)
	if err != nil {
		return err
	}

	fresh := state.Empty()
	counter := state.Value()
	if fresh {
		counter.Word = input.Word
	}
	counter.Count++
	return nil
}

// [END word_count_mapper]
