package main

import (
	"context"
	"errors"

	"go.ytsaurus.tech/yt/go/flow"
)

const enrichedStreamID = "enriched"

// [BEGIN enricher]

type enricher struct{}

var _ flow.RowFunction = (*enricher)(nil)

func (*enricher) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var event eventMessage
	if err := msg.ConvertTo(&event); err != nil {
		return err
	}

	name, joined, err := joinedName(rt, msg)
	if err != nil || !joined {
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

func joinedName(rt flow.Runtime, msg flow.ExtendedMessage) (string, bool, error) {
	state, err := flow.OpenJoinedExternalState(rt, referenceStateName, msg)
	if errors.Is(err, flow.ErrStateNotRead) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	var reference referenceState
	exists, err := state.ConvertTo(&reference)
	if err != nil || !exists || reference.NormalizedName == nil {
		return "", false, err
	}
	return *reference.NormalizedName, true, nil
}

// [END enricher]
