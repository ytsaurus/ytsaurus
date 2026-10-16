package main

import (
	"context"

	"go.ytsaurus.tech/yt/go/flow"
)

// [BEGIN request_processor]

type requestProcessor struct{}

var _ flow.RowFunction = (*requestProcessor)(nil)

func (*requestProcessor) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var request requestMessage
	if err := msg.ConvertTo(&request); err != nil {
		return err
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
	return nil
}

// [END request_processor]
