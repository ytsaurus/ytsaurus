package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

const (
	hitID      = "h1"
	hitPayload = "p1"

	hitTime   uint64 = 1000
	closeTime        = hitTime + 10
)

func newHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("join", &joinFunction{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			hitStream:    flowtest.Schema("hit_id:string", "hit_time:uint64", "hit_payload:string"),
			actionStream: flowtest.Schema("hit_id:string", "hit_time:uint64", "action_time:uint64", "is_click:boolean"),
			joinedStream: flowtest.Schema(
				"hit_id:string", "hit_time:uint64", "is_click:boolean",
				"show_time:uint64", "click_time:uint64", "hit_payload:string",
			),
		},
		KeySchema: flowtest.Schema("hit_id:string", "hit_time:uint64"),
		ExternalStates: map[string]flow.Schema{
			joinStateName: flowtest.Schema("show_time:uint64", "click_time:uint64", "hit_payload:string"),
		},
		Parameters: map[string]any{waitForActionsParameter: "10s"},
	})
}

func TestHitStoresItsPayloadAndArmsTheClosingTimer(t *testing.T) {
	h := newHarness(t)

	r := h.Process(hit(h, hitTime))

	require.Equal(t, flowtest.Row{"hit_payload": hitPayload}, r.ExternalStateRow(joinStateName, key(h)))
	require.Equal(t, []flow.TimerRequest{{TriggerTimestamp: closeTime, EventTimestamp: hitTime}}, r.Timers())
}

func TestShowAndClickAreStoredApart(t *testing.T) {
	h := newHarness(t)

	r := h.Process(action(h, hitTime+2, false), action(h, hitTime+5, true))

	require.Equal(t, flowtest.Row{
		"show_time":  hitTime + 2,
		"click_time": hitTime + 5,
	}, r.ExternalStateRow(joinStateName, key(h)))
}

func TestMessageBeyondTheWindowIsDropped(t *testing.T) {
	h := newHarness(t)

	r := h.Process(action(h, closeTime, true))

	require.False(t, r.ExternalStateWritten(joinStateName))
	require.Empty(t, r.Timers())
}

func TestLateMessageIsDropped(t *testing.T) {
	h := newHarness(t)
	h.SetWatermark(hitStream, hitTime+3)
	h.SetWatermark(actionStream, hitTime+3)

	r := h.Process(hit(h, hitTime))

	require.False(t, r.ExternalStateWritten(joinStateName))
	require.Empty(t, r.Timers())
}

func TestClosingTheWindowPublishesTheJoinedAction(t *testing.T) {
	h := newHarness(t)

	h.Process(hit(h, hitTime), action(h, hitTime+2, false), action(h, hitTime+5, true))
	r := h.Process(closingTimer(h))

	require.Len(t, r.MessagesOn(joinedStream), 1)
	require.Equal(t, []flowtest.Row{{
		"hit_id":      hitID,
		"hit_time":    hitTime,
		"show_time":   hitTime + 2,
		"click_time":  hitTime + 5,
		"is_click":    true,
		"hit_payload": hitPayload,
	}}, r.Rows())
	require.True(t, r.ExternalStateReset(joinStateName, key(h)))
}

func TestAWindowWithoutAClickIsPublishedAsAShow(t *testing.T) {
	h := newHarness(t)

	h.Process(hit(h, hitTime), action(h, hitTime+2, false))
	r := h.Process(closingTimer(h))

	require.Equal(t, []flowtest.Row{{
		"hit_id":      hitID,
		"hit_time":    hitTime,
		"show_time":   hitTime + 2,
		"click_time":  uint64(0),
		"is_click":    false,
		"hit_payload": hitPayload,
	}}, r.Rows())
}

func TestAWindowWithoutAShowPublishesNothing(t *testing.T) {
	h := newHarness(t)

	h.Process(hit(h, hitTime))
	r := h.Process(closingTimer(h))

	require.Empty(t, r.Messages())
	require.True(t, r.ExternalStateReset(joinStateName, key(h)))
}

func TestAWindowWithoutTheHitPublishesNothing(t *testing.T) {
	h := newHarness(t)

	h.Process(action(h, hitTime+2, false))
	r := h.Process(closingTimer(h))

	require.Empty(t, r.Messages())
	require.True(t, r.ExternalStateReset(joinStateName, key(h)))
}

func key(h *flowtest.Harness) flow.Payload {
	return h.Key(flowtest.Row{"hit_id": hitID, "hit_time": hitTime})
}

func hit(h *flowtest.Harness, eventTimestamp uint64) flow.ExtendedMessage {
	msg := h.KeyedMessage(hitStream, key(h), flowtest.Row{
		"hit_id":      hitID,
		"hit_time":    hitTime,
		"hit_payload": hitPayload,
	})
	msg.EventTimestamp = eventTimestamp
	return msg
}

func action(h *flowtest.Harness, actionTime uint64, isClick bool) flow.ExtendedMessage {
	msg := h.KeyedMessage(actionStream, key(h), flowtest.Row{
		"hit_id":      hitID,
		"hit_time":    hitTime,
		"action_time": actionTime,
		"is_click":    isClick,
	})
	msg.EventTimestamp = actionTime
	return msg
}

func closingTimer(h *flowtest.Harness) flow.Timer {
	timer := h.Timer(key(h), closeTime)
	timer.StreamID = timerStream
	return timer
}
