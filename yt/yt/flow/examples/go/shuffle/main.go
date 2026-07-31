package main

import (
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[event](eventStreamID),
		flow.NewYSONStream[event]("event_a"),
		flow.NewYSONStream[event]("event_b"),
		flow.NewYSONStream[event]("event_c"),
		flow.NewYSONStream[event]("event_d"),
	)
	pipeline.Add(
		flow.NewRowSourceComputation("reader", &eventMapper{}),
		flow.NewRowComputation("reducer", &eventReducer{}),
	)

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "shuffle: %v\n", err)
		os.Exit(1)
	}
}
