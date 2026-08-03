package main

import (
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

type eventMessage struct {
	flow.YSONMessage
	Key  uint64 `yson:"key"`
	Data string `yson:"data"`
}

type requestMessage struct {
	flow.YSONMessage
	RequestID uint64 `yson:"request_id"`
	Key       uint64 `yson:"key"`
	Request   string `yson:"request"`
}

type responseMessage struct {
	flow.YSONMessage
	RequestID uint64 `yson:"request_id"`
	Key       uint64 `yson:"key"`
	Length    int64  `yson:"length"`
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[eventMessage](eventStreamID),
		flow.NewYSONStream[requestMessage](requestStreamID),
		flow.NewYSONStream[responseMessage](responseStreamID),
	)
	pipeline.Add(
		flow.NewRowComputation("state", &stateKeeper{}),
		flow.NewRowComputation("processor", &requestProcessor{}),
	)

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "retryable_async_request: %v\n", err)
		os.Exit(1)
	}
}
