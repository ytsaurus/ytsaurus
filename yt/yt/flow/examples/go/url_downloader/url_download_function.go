package main

import (
	"context"
	"fmt"
	"time"
	"unicode"

	"go.ytsaurus.tech/yt/go/flow"
)

const hostStateName = "host-state"

const outputStreamID = "processed_urls"

const flushDelay = 5 * time.Second

// [BEGIN host_state]

type hostState struct {
	Host        string   `yson:"host"`
	PendingURLs []string `yson:"pending_urls"`
}

// [END host_state]

// [BEGIN url_download_function]

type urlDownloadFunction struct{}

var (
	_ flow.RowFunction      = (*urlDownloadFunction)(nil)
	_ flow.RowTimerFunction = (*urlDownloadFunction)(nil)
)

func (*urlDownloadFunction) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input urlMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	state, err := flow.OpenYSONState[hostState](rt, hostStateName, msg)
	if err != nil {
		return err
	}

	fresh := state.Empty()
	batch := state.Value()
	if fresh {
		batch.Host = input.Host
	}
	batch.PendingURLs = append(batch.PendingURLs, input.URL)

	out.AddTimer(flow.TimerRequest{TriggerTimestamp: uint64(time.Now().Add(flushDelay).Unix())})
	return nil
}

func (*urlDownloadFunction) OnTimer(
	ctx context.Context,
	rt flow.Runtime,
	timer flow.Timer,
	out flow.OutputCollector,
) error {
	state, err := flow.OpenYSONState[hostState](rt, hostStateName, timer)
	if err != nil {
		return err
	}

	if state.Empty() {
		return nil
	}
	batch := state.Value()
	if len(batch.PendingURLs) == 0 {
		state.Clear()
		return nil
	}

	for _, url := range batch.PendingURLs {
		processed := flow.NewYSONMessage[processedURLMessage](outputStreamID)
		processed.Host = batch.Host
		processed.URL = url
		processed.Data = processURL(url)
		encoded, err := flow.ConvertFrom(rt, processed)
		if err != nil {
			return err
		}
		out.AddMessage(encoded)
	}

	state.Clear()
	return nil
}

// [END url_download_function]

func processURL(url string) string {
	digits := 0
	for _, r := range url {
		if unicode.IsDigit(r) {
			digits++
		}
	}
	return fmt.Sprintf("length: %d, digits: %d", len(url), digits)
}
