package agent

import (
	"fmt"
	"sync"
	"time"

	"go.ytsaurus.tech/yt/go/yterrors"
)

// processHealthState represents a state of one background process (pass/track_nodes/etc).
type processHealthState struct {
	// name of the process for better logging/error purposes.
	name string
	// expirationTimeout sets the amount of time the state is valid after modification.
	// If the state hasn't been updated for expiration timeout, it is considered to be expired.
	//
	// Zero means no timeout.
	expirationTimeout time.Duration

	error                    error
	lastModificationTime     time.Time
	previousModificationTime time.Time
}

func makeProcessHealthState(name string, timeout time.Duration) processHealthState {
	now := time.Now()
	return processHealthState{
		name:              name,
		expirationTimeout: timeout,

		error:                    yterrors.Err("health state is not set"),
		lastModificationTime:     now,
		previousModificationTime: now,
	}
}

func (h *processHealthState) Get() error {
	if h.error != nil {
		return h.error
	}
	if h.expirationTimeout != 0 {
		elapsedTime := time.Since(h.lastModificationTime)
		if elapsedTime > h.expirationTimeout {
			return yterrors.Err("health state has expired",
				yterrors.Attr("process_name", h.name),
				yterrors.Attr("elapsed_time", elapsedTime),
				yterrors.Attr("previous_elapsed_time", h.lastModificationTime.Sub(h.previousModificationTime)),
				yterrors.Attr("timeout", h.expirationTimeout))
		}
	}
	return nil
}

func (h *processHealthState) Set(err error) {
	if err != nil {
		h.error = yterrors.Err(fmt.Sprintf("%v process has failed", h.name), err)
	} else {
		h.error = nil
	}
	h.previousModificationTime = h.lastModificationTime
	h.lastModificationTime = time.Now()
}

// agentHealthState combines health states for all agent's processes.
//
// An agentHealthState is safe for concurrent use by multiple goroutines.
type agentHealthState struct {
	initState       processHealthState
	passState       processHealthState
	trackNodesState processHealthState
	trackOpsState   processHealthState
	m               sync.Mutex
}

func newAgentHealthState(passStateTimeout, trackNodesStateTimeout, trackOpsStateTimeout time.Duration) *agentHealthState {
	return &agentHealthState{
		initState:       makeProcessHealthState("init", 0),
		passState:       makeProcessHealthState("pass", passStateTimeout),
		trackNodesState: makeProcessHealthState("track_nodes", trackNodesStateTimeout),
		trackOpsState:   makeProcessHealthState("track_ops", trackOpsStateTimeout),
		m:               sync.Mutex{},
	}
}

func (s *agentHealthState) SetInitState(err error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.initState.Set(err)
}

func (s *agentHealthState) SetPassState(err error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.passState.Set(err)
}

func (s *agentHealthState) SetTrackNodesState(err error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.trackNodesState.Set(err)
}

func (s *agentHealthState) SetTrackOpsState(err error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.trackOpsState.Set(err)
}

func (s *agentHealthState) Get() error {
	s.m.Lock()
	defer s.m.Unlock()

	// Explicit error is prioritized over `state's been expired` error.
	if s.initState.error != nil {
		return s.initState.error
	}
	if s.passState.error != nil {
		return s.passState.error
	}
	if s.trackNodesState.error != nil {
		return s.trackNodesState.error
	}
	if s.trackOpsState.error != nil {
		return s.trackOpsState.error
	}

	if err := s.initState.Get(); err != nil {
		return err
	}
	if err := s.passState.Get(); err != nil {
		return err
	}
	if err := s.trackNodesState.Get(); err != nil {
		return err
	}
	if err := s.trackOpsState.Get(); err != nil {
		return err
	}
	return nil
}
