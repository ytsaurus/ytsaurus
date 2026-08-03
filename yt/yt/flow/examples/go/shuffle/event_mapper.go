package main

import (
	"context"
	"encoding/json"
	"fmt"

	"go.ytsaurus.tech/yt/go/flow"
)

const eventStreamID = "event"

// [BEGIN event]

type sourceMessage struct {
	flow.YSONMessage
	Data []byte `yson:"data"`
}

type event struct {
	flow.YSONMessage
	KeyA  uint64 `json:"key_a" yson:"key_a"`
	KeyB  uint64 `json:"key_b" yson:"key_b"`
	KeyC  uint64 `json:"key_c" yson:"key_c"`
	KeyD  uint64 `json:"key_d" yson:"key_d"`
	Value string `json:"value" yson:"value"`
}

// [END event]

// [BEGIN event_mapper]

type eventMapper struct{}

var _ flow.RowFunction = (*eventMapper)(nil)

func (*eventMapper) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input sourceMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	parsed := flow.NewYSONMessage[event](eventStreamID)
	if err := json.Unmarshal(input.Data, parsed); err != nil {
		return fmt.Errorf("parsing the data column: %w", err)
	}
	parsed.Meta.StreamID = eventStreamID
	message, err := flow.ConvertFrom(rt, parsed)
	if err != nil {
		return err
	}

	out.AddMessage(message)
	return nil
}

// [END event_mapper]
