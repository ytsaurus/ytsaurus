package main

import (
	"context"
	"errors"

	"go.ytsaurus.tech/yt/go/flow"
)

const (
	referenceStateName = "/reference"
	enrichedStreamID   = "enriched"
)

// [BEGIN lookup_join]

type referenceState struct {
	Name *string `yson:"name"`
}

type lookupJoin struct{}

var _ flow.RowFunction = (*lookupJoin)(nil)

func (*lookupJoin) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var event eventMessage
	if err := msg.ConvertTo(&event); err != nil {
		return err
	}

	name, ok, err := referenceName(rt, msg)
	if err != nil || !ok {
		return err
	}

	enriched := flow.NewYSONMessage[enrichedMessage](enrichedStreamID)
	enriched.Key = event.Key
	enriched.Name = name
	encoded, err := flow.ConvertFrom(rt, enriched)
	if err != nil {
		return err
	}
	out.AddMessage(encoded)
	return nil
}

func referenceName(rt flow.Runtime, msg flow.ExtendedMessage) (string, bool, error) {
	reference, err := flow.OpenJoinedExternalState(rt, referenceStateName, msg)
	if errors.Is(err, flow.ErrStateNotRead) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	var state referenceState
	exists, err := reference.ConvertTo(&state)
	if err != nil || !exists || state.Name == nil {
		return "", false, err
	}
	return *state.Name, true, nil
}

// [END lookup_join]
