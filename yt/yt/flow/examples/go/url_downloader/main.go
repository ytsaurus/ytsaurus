package main

import (
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

type urlMessage struct {
	flow.YSONMessage
	Host string `yson:"host"`
	URL  string `yson:"url"`
}

type processedURLMessage struct {
	flow.YSONMessage
	Host string `yson:"host"`
	URL  string `yson:"url"`
	Data string `yson:"data"`
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[urlMessage]("urls"),
		flow.NewYSONStream[processedURLMessage](outputStreamID),
	)
	pipeline.Add(flow.NewRowComputation("url_downloader", &urlDownloadFunction{}))

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "url_downloader: %v\n", err)
		os.Exit(1)
	}
}
