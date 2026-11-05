package main

import (
	"context"

	"go.ytsaurus.tech/yt/go/flow"
)

// reader receives the records of the source and emits nothing; put your logic here.
type reader struct{}

var _ flow.RowFunction = (*reader)(nil)

func (*reader) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	return nil
}
