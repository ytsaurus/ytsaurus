package main

import (
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

func main() {
	pipeline := flow.NewPipeline()
	pipeline.Add(flow.NewRowSourceComputation("reader", &reader{}))

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "noop: %v\n", err)
		os.Exit(1)
	}
}
