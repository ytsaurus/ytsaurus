package main

import (
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

const referenceStateName = "/reference_state"

type referenceMessage struct {
	flow.YSONMessage
	Key  uint64 `yson:"key"`
	Name string `yson:"name"`
}

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
		flow.NewYSONStream[referenceMessage]("reference"),
		flow.NewYSONStream[eventMessage]("event"),
		flow.NewYSONStream[enrichedMessage](enrichedStreamID),
	)
	pipeline.Add(
		flow.NewRowComputation("reference_loader", &referenceLoader{}),
		flow.NewRowComputation("enricher", &enricher{}),
	)

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "static_table_join: %v\n", err)
		os.Exit(1)
	}
}
