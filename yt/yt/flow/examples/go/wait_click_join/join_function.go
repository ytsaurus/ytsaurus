package main

import (
	"context"
	"fmt"
	"time"

	"go.ytsaurus.tech/yt/go/flow"
)

const (
	hitStream    = "hit"
	actionStream = "action"
	joinedStream = "joined_action"
	timerStream  = "timer"
)

const joinStateName = "/join-state"

const waitForActionsParameter = "wait_for_actions"

type joinState struct {
	ShowTime   *uint64 `yson:"show_time"`
	ClickTime  *uint64 `yson:"click_time"`
	HitPayload *string `yson:"hit_payload"`
}

type joinKey struct {
	HitID   string `yson:"hit_id"`
	HitTime uint64 `yson:"hit_time"`
}

// [BEGIN join_function]

type joinFunction struct{}

var (
	_ flow.RowFunction      = (*joinFunction)(nil)
	_ flow.RowTimerFunction = (*joinFunction)(nil)
)

func (*joinFunction) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var hit *hitMessage
	var action *actionMessage
	var hitTime uint64
	switch msg.StreamID {
	case hitStream:
		hit = &hitMessage{}
		if err := msg.ConvertTo(hit); err != nil {
			return err
		}
		hitTime = hit.HitTime
	case actionStream:
		action = &actionMessage{}
		if err := msg.ConvertTo(action); err != nil {
			return err
		}
		hitTime = action.HitTime
	default:
		return fmt.Errorf("unhandled stream %q", msg.StreamID)
	}

	wait, err := waitForActions(rt)
	if err != nil {
		return err
	}
	closeTime := hitTime + wait
	if msg.EventTimestamp >= closeTime || msg.EventTimestamp < rt.MinWatermark() {
		return nil
	}

	state, err := flow.OpenExternalState(rt, joinStateName, msg)
	if err != nil {
		return err
	}
	var window joinState
	if _, err := state.ConvertTo(&window); err != nil {
		return err
	}

	if hit != nil {
		window.HitPayload = &hit.HitPayload
	} else if action.IsClick {
		window.ClickTime = &action.ActionTime
	} else {
		window.ShowTime = &action.ActionTime
	}
	if err := state.ConvertFrom(&window); err != nil {
		return err
	}

	out.AddTimer(flow.TimerRequest{TriggerTimestamp: closeTime, EventTimestamp: hitTime})
	return nil
}

// [END join_function]

// [BEGIN on_timer]

func (*joinFunction) OnTimer(
	ctx context.Context,
	rt flow.Runtime,
	timer flow.Timer,
	out flow.OutputCollector,
) error {
	if timer.StreamID != timerStream {
		return fmt.Errorf("unhandled timer stream %q", timer.StreamID)
	}

	state, err := flow.OpenExternalState(rt, joinStateName, timer)
	if err != nil {
		return err
	}
	var window joinState
	if _, err := state.ConvertTo(&window); err != nil {
		return err
	}

	if window.ShowTime != nil && *window.ShowTime != 0 && window.HitPayload != nil {
		var key joinKey
		if err := timer.Key.ConvertTo(&key); err != nil {
			return err
		}
		joined, err := joinedAction(rt, key, window)
		if err != nil {
			return err
		}
		out.AddMessage(joined)
	}

	return state.Clear()
}

// [END on_timer]

func joinedAction(rt flow.Runtime, key joinKey, window joinState) (flow.Message, error) {
	var clickTime uint64
	if window.ClickTime != nil {
		clickTime = *window.ClickTime
	}

	joined := flow.NewYSONMessage[joinedActionMessage](joinedStream)
	joined.HitID = key.HitID
	joined.HitTime = key.HitTime
	joined.ShowTime = *window.ShowTime
	joined.ClickTime = clickTime
	joined.IsClick = clickTime != 0
	joined.HitPayload = *window.HitPayload
	return flow.ConvertFrom(rt, joined)
}

func waitForActions(rt flow.Runtime) (uint64, error) {
	var spelled string
	if err := rt.Parameters().Get(waitForActionsParameter, &spelled); err != nil {
		return 0, err
	}

	wait, err := time.ParseDuration(spelled)
	if err != nil {
		return 0, fmt.Errorf("parameter %q: %w", waitForActionsParameter, err)
	}
	return uint64(wait.Seconds()), nil
}
