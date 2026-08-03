package main

import (
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

type eventMessage struct {
	flow.YSONMessage
	Key uint64 `yson:"key"`
}

type enrichedMessage struct {
	flow.YSONMessage
	Key  uint64 `yson:"key"`
	Name string `yson:"name"`
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[eventMessage]("event"),
		flow.NewYSONStream[enrichedMessage](enrichedStreamID),
	)
	pipeline.Add(flow.NewRowComputation("lookup_join", &lookupJoin{}))

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "external_state_join: %v\n", err)
		os.Exit(1)
	}
}
