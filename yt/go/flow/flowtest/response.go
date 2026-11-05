package flowtest

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/yson"
)

// Response contains the output and resulting states of one run.
type Response struct {
	tb      testing.TB
	results []flow.OutputGroup
	runtime *flow.RequestRuntime
}

// Groups returns the output groups in the order they were produced.
func (r *Response) Groups() []flow.OutputGroup {
	return r.results
}

// Messages returns the output messages of every group, in order.
func (r *Response) Messages() []flow.Message {
	var messages []flow.Message
	for _, group := range r.results {
		messages = append(messages, group.Messages...)
	}
	return messages
}

// MessagesOn returns the output messages on one stream, in order.
func (r *Response) MessagesOn(streamID string) []flow.Message {
	var messages []flow.Message
	for _, group := range r.results {
		for _, msg := range group.Messages {
			if msg.StreamID == streamID {
				messages = append(messages, msg)
			}
		}
	}
	return messages
}

// Rows returns output payloads decoded by ToRow.
func (r *Response) Rows() []Row {
	messages := r.Messages()
	rows := make([]Row, 0, len(messages))
	for _, msg := range messages {
		rows = append(rows, ToRow(msg.Payload))
	}
	return rows
}

// Distribute returns downstream delivery flags aligned with Messages.
func (r *Response) Distribute() []bool {
	var distribute []bool
	for _, group := range r.results {
		for i := range group.Messages {
			distribute = append(distribute, group.DistributeAt(i))
		}
	}
	return distribute
}

// Timers returns the timers the computation asked the worker to set, in order.
func (r *Response) Timers() []flow.TimerRequest {
	var timers []flow.TimerRequest
	for _, group := range r.results {
		timers = append(timers, group.Timers...)
	}
	return timers
}

// InternalStateRaw returns the bytes an internal state holds for a key.
func (r *Response) InternalStateRaw(name string, key flow.Payload) ([]byte, bool) {
	state, ok := r.internalHolder(name).Get(key)
	if !ok || state.Reset {
		return nil, false
	}
	return state.Data, true
}

// InternalStateYSON deserializes into dst the YSON an internal state holds for a key.
func (r *Response) InternalStateYSON(name string, key flow.Payload, dst any) bool {
	r.tb.Helper()

	data, ok := r.InternalStateRaw(name, key)
	if !ok {
		return false
	}
	if err := yson.Unmarshal(data, dst); err != nil {
		r.tb.Fatalf("flowtest: internal state %q: %v", name, err)
		return false
	}
	return true
}

// InternalStateProto deserializes a protobuf internal state.
func (r *Response) InternalStateProto(name string, key flow.Payload, dst proto.Message) bool {
	r.tb.Helper()

	data, ok := r.InternalStateRaw(name, key)
	if !ok {
		return false
	}
	if err := proto.Unmarshal(data, dst); err != nil {
		r.tb.Fatalf("flowtest: internal state %q: %v", name, err)
		return false
	}
	return true
}

// InternalStateReset reports whether the run cleared the internal state of a key.
func (r *Response) InternalStateReset(name string, key flow.Payload) bool {
	state, ok := r.internalHolder(name).Get(key)
	return ok && state.Reset
}

// InternalStateLen returns the number of keys an internal state was read or written for.
func (r *Response) InternalStateLen(name string) int {
	return r.internalHolder(name).Len()
}

// InternalStateWritten reports whether the run wrote the named internal state.
func (r *Response) InternalStateWritten(name string) bool {
	return r.internalHolder(name).HasModified()
}

// ExternalState returns the row an external state the computation owns holds for a key.
func (r *Response) ExternalState(name string, key flow.Payload) (flow.Payload, bool) {
	return externalState(r.externalHolder(name), key)
}

// ExternalStateRow returns an external state row decoded by ToRow.
func (r *Response) ExternalStateRow(name string, key flow.Payload) Row {
	value, ok := r.ExternalState(name, key)
	if !ok {
		return nil
	}
	return ToRow(value)
}

// ExternalStateReset reports whether the run deleted the external state row of a key.
func (r *Response) ExternalStateReset(name string, key flow.Payload) bool {
	holder := r.externalHolder(name)
	if holder == nil {
		return false
	}
	state, ok := holder.Get(key)
	return ok && state.Reset
}

// ExternalStateLen returns the number of keys an external state was read or written for.
func (r *Response) ExternalStateLen(name string) int {
	holder := r.externalHolder(name)
	if holder == nil {
		return 0
	}
	return holder.Len()
}

// ExternalStateWritten reports whether the run wrote the named external state.
func (r *Response) ExternalStateWritten(name string) bool {
	holder := r.externalHolder(name)
	return holder != nil && holder.HasModified()
}

// JoinedExternalState returns the row a joined external state holds for a key.
func (r *Response) JoinedExternalState(name string, key flow.Payload) (flow.Payload, bool) {
	return externalState(r.joinedHolder(name), key)
}

// JoinedExternalStateRow returns a joined state row decoded by ToRow.
func (r *Response) JoinedExternalStateRow(name string, key flow.Payload) Row {
	value, ok := r.JoinedExternalState(name, key)
	if !ok {
		return nil
	}
	return ToRow(value)
}

func externalState(holder *flow.StatesHolder[flow.ExternalState], key flow.Payload) (flow.Payload, bool) {
	if holder == nil {
		return flow.Payload{}, false
	}
	state, ok := holder.Get(key)
	if !ok || state.Reset || state.Value.Row() == nil {
		return flow.Payload{}, false
	}
	return state.Value, true
}

func (r *Response) internalHolder(name string) *flow.StatesHolder[flow.InternalState] {
	r.tb.Helper()

	holder, err := r.runtime.InternalState(name)
	if err != nil {
		r.tb.Fatalf("flowtest: %v", err)
		return nil
	}
	return holder
}

func (r *Response) externalHolder(name string) *flow.StatesHolder[flow.ExternalState] {
	r.tb.Helper()
	return r.stateHolder(name, r.runtime.ExternalState)
}

func (r *Response) joinedHolder(name string) *flow.StatesHolder[flow.ExternalState] {
	r.tb.Helper()
	return r.stateHolder(name, r.runtime.JoinedExternalState)
}

func (r *Response) stateHolder(
	name string,
	open func(string) (*flow.StatesHolder[flow.ExternalState], error),
) *flow.StatesHolder[flow.ExternalState] {
	r.tb.Helper()

	holder, err := open(name)
	switch {
	case err == nil:
		return holder
	case errors.Is(err, flow.ErrStateNotRead):
		return nil
	default:
		r.tb.Fatalf("flowtest: %v", err)
		return nil
	}
}
