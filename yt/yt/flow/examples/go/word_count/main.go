package main

import (
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

type wordMessage struct {
	flow.YSONMessage
	Word string `yson:"word"`
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(flow.NewYSONStream[wordMessage]("words"))
	pipeline.Add(flow.NewRowComputation("mapper", &wordCountMapper{}))

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "word_count: %v\n", err)
		os.Exit(1)
	}
}
