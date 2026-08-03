package main

import (
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

type actionMessage struct {
	flow.YSONMessage
	HitID      string `yson:"hit_id"`
	HitTime    uint64 `yson:"hit_time"`
	IsClick    bool   `yson:"is_click"`
	ActionTime uint64 `yson:"action_time"`
}

type hitMessage struct {
	flow.YSONMessage
	HitID      string `yson:"hit_id"`
	HitTime    uint64 `yson:"hit_time"`
	HitPayload string `yson:"hit_payload"`
}

type joinedActionMessage struct {
	flow.YSONMessage
	HitID      string `yson:"hit_id"`
	HitTime    uint64 `yson:"hit_time"`
	IsClick    bool   `yson:"is_click"`
	ShowTime   uint64 `yson:"show_time"`
	ClickTime  uint64 `yson:"click_time"`
	HitPayload string `yson:"hit_payload"`
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[actionMessage](actionStream),
		flow.NewYSONStream[hitMessage](hitStream),
		flow.NewYSONStream[joinedActionMessage](joinedStream),
	)
	pipeline.Add(flow.NewRowComputation("join", &joinFunction{}))

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "wait_click_join: %v\n", err)
		os.Exit(1)
	}
}
