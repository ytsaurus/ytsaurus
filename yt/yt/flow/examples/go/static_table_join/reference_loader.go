package main

import (
	"context"
	"strings"

	"go.ytsaurus.tech/yt/go/flow"
)

type referenceState struct {
	NormalizedName *string `yson:"normalized_name"`
}

type referenceLoader struct{}

var _ flow.RowFunction = (*referenceLoader)(nil)

func (*referenceLoader) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var reference referenceMessage
	if err := msg.ConvertTo(&reference); err != nil {
		return err
	}

	state, err := flow.OpenExternalState(rt, referenceStateName, msg)
	if err != nil {
		return err
	}

	normalized := normalize(reference.Name)
	return state.ConvertFrom(&referenceState{NormalizedName: &normalized})
}

func normalize(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
