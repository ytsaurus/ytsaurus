package flow

import (
	"bytes"
	"iter"
	"slices"

	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/yson"
)

// ErrUnknownStream reports a stream the job exchanges no messages on.
var ErrUnknownStream = xerrors.NewSentinel("unknown stream")

// RequestRuntime holds the runtime data of one request.
type RequestRuntime struct {
	job     *Job
	streams StreamSpecs

	watermarks   map[string]uint64
	minWatermark uint64
	watermarked  bool

	internal map[string]*StatesHolder[InternalState]
	external map[string]*StatesHolder[ExternalState]
	joined   map[string]*StatesHolder[ExternalState]

	trackedStates     map[trackedStateKey]trackedState
	trackedStateOrder []trackedState
}

var _ Runtime = (*RequestRuntime)(nil)

// NewRequestRuntime returns an empty runtime for a job.
func NewRequestRuntime(job *Job) *RequestRuntime {
	return &RequestRuntime{
		job:           job,
		streams:       job.StreamSpecs(),
		watermarks:    map[string]uint64{},
		internal:      map[string]*StatesHolder[InternalState]{},
		external:      map[string]*StatesHolder[ExternalState]{},
		joined:        map[string]*StatesHolder[ExternalState]{},
		trackedStates: map[trackedStateKey]trackedState{},
	}
}

// SetStreamSpecs replaces the streams for this request.
func (r *RequestRuntime) SetStreamSpecs(streams StreamSpecs) {
	r.streams = streams
}

// SetWatermark records a stream watermark.
func (r *RequestRuntime) SetWatermark(streamID string, watermark uint64) {
	r.watermarks[streamID] = watermark
	if !r.watermarked || watermark < r.minWatermark {
		r.minWatermark = watermark
		r.watermarked = true
	}
}

// LoadInternalState loads an internal state entry.
func (r *RequestRuntime) LoadInternalState(name string, key Payload, value InternalState) error {
	holder, err := r.InternalState(name)
	if err != nil {
		return err
	}
	return holder.Load(key, value)
}

// LoadExternalState loads an owned external state entry.
func (r *RequestRuntime) LoadExternalState(name string, stateSchema Schema, key Payload, value ExternalState) error {
	holder, err := r.loadExternalHolder(r.external, name, stateSchema, r.job.ValidateExternalStateName)
	if err != nil {
		return err
	}
	return holder.Load(key, value)
}

// LoadJoinedExternalState loads a joined external state entry.
func (r *RequestRuntime) LoadJoinedExternalState(name string, stateSchema Schema, key Payload, value ExternalState) error {
	holder, err := r.loadExternalHolder(r.joined, name, stateSchema, r.job.ValidateJoinedExternalStateName)
	if err != nil {
		return err
	}
	return holder.Load(key, value)
}

// ModifiedInternalStates iterates over modified internal states by name.
func (r *RequestRuntime) ModifiedInternalStates() iter.Seq[*StatesHolder[InternalState]] {
	return modifiedHolders(r.internal)
}

// ModifiedExternalStates iterates over modified external states by name.
func (r *RequestRuntime) ModifiedExternalStates() iter.Seq[*StatesHolder[ExternalState]] {
	return modifiedHolders(r.external)
}

// Parameters returns the parameters of the computation's static spec.
func (r *RequestRuntime) Parameters() Parameters {
	return r.job.StaticParameters()
}

// DynamicParameters returns the parameters of the computation's dynamic spec.
func (r *RequestRuntime) DynamicParameters() Parameters {
	return r.job.DynamicParameters()
}

// KeySchema returns the schema of the key the inputs are grouped by.
func (r *RequestRuntime) KeySchema() Schema {
	return r.job.GroupBySchema()
}

// StreamSpecs returns the streams of this request.
func (r *RequestRuntime) StreamSpecs() StreamSpecs {
	return r.streams
}

// MessageBuilder returns a builder for an output stream.
func (r *RequestRuntime) MessageBuilder(streamID string) (*MessageBuilder, error) {
	stream, ok := r.streams.Stream(streamID)
	if !ok {
		return nil, xerrors.Errorf("flow: %w: %q", ErrUnknownStream, streamID)
	}
	return newMessageBuilder(streamID, stream.Schema), nil
}

// MinWatermark returns the lowest watermark, or zero if none was reported.
func (r *RequestRuntime) MinWatermark() uint64 {
	return r.minWatermark
}

// Watermark returns the event watermark of one stream.
func (r *RequestRuntime) Watermark(streamID string) (uint64, bool) {
	watermark, ok := r.watermarks[streamID]
	return watermark, ok
}

// InternalState returns the named internal state.
func (r *RequestRuntime) InternalState(name string) (*StatesHolder[InternalState], error) {
	if err := r.job.ValidateInternalStateName(name); err != nil {
		return nil, err
	}
	if holder, ok := r.internal[name]; ok {
		return holder, nil
	}
	holder := newInternalStatesHolder(name)
	r.internal[name] = holder
	return holder, nil
}

// ExternalState returns the named owned external state.
func (r *RequestRuntime) ExternalState(name string) (*StatesHolder[ExternalState], error) {
	return externalHolder(r.external, name, r.job.ValidateExternalStateName)
}

// JoinedExternalState returns the named joined external state.
func (r *RequestRuntime) JoinedExternalState(name string) (*StatesHolder[ExternalState], error) {
	return externalHolder(r.joined, name, r.job.ValidateJoinedExternalStateName)
}

func (r *RequestRuntime) loadExternalHolder(
	holders map[string]*StatesHolder[ExternalState],
	name string,
	stateSchema Schema,
	validate func(string) error,
) (*StatesHolder[ExternalState], error) {
	if err := validate(name); err != nil {
		return nil, err
	}
	if holder, ok := holders[name]; ok {
		return holder, nil
	}
	holder, err := newExternalStatesHolder(name, stateSchema)
	if err != nil {
		return nil, err
	}
	holders[name] = holder
	return holder, nil
}

func externalHolder(
	holders map[string]*StatesHolder[ExternalState],
	name string,
	validate func(string) error,
) (*StatesHolder[ExternalState], error) {
	if err := validate(name); err != nil {
		return nil, err
	}
	holder, ok := holders[name]
	if !ok {
		return nil, xerrors.Errorf("flow: %w: state %q", ErrStateNotRead, name)
	}
	return holder, nil
}

type trackedStateKey struct {
	holder *StatesHolder[InternalState]
	key    string
}

type trackedState interface {
	flush() error
}

func (r *RequestRuntime) getTrackedState(
	key trackedStateKey,
	create func() (trackedState, error),
) (trackedState, error) {
	if state, ok := r.trackedStates[key]; ok {
		return state, nil
	}
	state, err := create()
	if err != nil {
		return nil, err
	}
	r.trackedStates[key] = state
	r.trackedStateOrder = append(r.trackedStateOrder, state)
	return state, nil
}

func (r *RequestRuntime) resetTrackedStates() {
	clear(r.trackedStates)
	r.trackedStateOrder = r.trackedStateOrder[:0]
}

func (r *RequestRuntime) flushTrackedStates() error {
	defer r.resetTrackedStates()
	for _, state := range r.trackedStateOrder {
		if err := state.flush(); err != nil {
			return err
		}
	}
	return nil
}

// openTrackedState returns the state of the input key, decoding it once per state and key.
func openTrackedState[S trackedState](
	rt Runtime,
	name string,
	input Input,
	load func(RawStateAccessor) (S, error),
) (S, error) {
	var zero S
	raw, err := OpenRawState(rt, name, input)
	if err != nil {
		return zero, err
	}
	encodedKey, err := encodeStateKey(raw.key)
	if err != nil {
		return zero, xerrors.Errorf("flow: state %q: %w", name, err)
	}
	key := trackedStateKey{holder: raw.holder, key: encodedKey}
	tracked, err := rt.getTrackedState(key, func() (trackedState, error) {
		return load(raw)
	})
	if err != nil {
		return zero, err
	}
	state, ok := tracked.(S)
	if !ok {
		return zero, xerrors.Errorf("flow: state %q for this key was opened with another Go type", name)
	}
	return state, nil
}

// setTrackedState writes data unless it repeats the bytes the state arrived with.
func setTrackedState(raw RawStateAccessor, initial, data []byte) error {
	if bytes.Equal(data, initial) {
		return nil
	}
	return raw.Set(data)
}

// clearTrackedState resets the state unless the request brought none.
func clearTrackedState(raw RawStateAccessor, initial []byte) error {
	if initial == nil {
		return nil
	}
	return raw.Clear()
}

func modifiedHolders[T StateValue[T]](holders map[string]*StatesHolder[T]) iter.Seq[*StatesHolder[T]] {
	names := make([]string, 0, len(holders))
	for name, holder := range holders {
		if holder.HasModified() {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	return func(yield func(*StatesHolder[T]) bool) {
		for _, name := range names {
			if !yield(holders[name]) {
				return
			}
		}
	}
}

// RawStateAccessor reads and writes an internal state as bytes.
type RawStateAccessor struct {
	holder *StatesHolder[InternalState]
	key    Payload
}

// OpenRawState binds an internal state to the key of the input being handled.
func OpenRawState(rt Runtime, name string, input Input) (RawStateAccessor, error) {
	holder, err := rt.InternalState(name)
	if err != nil {
		return RawStateAccessor{}, err
	}
	return RawStateAccessor{holder: holder, key: input.PartitionKey()}, nil
}

// Get returns a copy of the state stored for the key.
func (a RawStateAccessor) Get() ([]byte, bool) {
	state, ok := a.holder.Get(a.key)
	if !ok || state.Reset {
		return nil, false
	}
	return bytes.Clone(state.Data), true
}

// Or returns the state stored for the key, or fallback if it has none.
func (a RawStateAccessor) Or(fallback []byte) []byte {
	if data, ok := a.Get(); ok {
		return data
	}
	return fallback
}

// Set copies data into the state of the key; empty data deletes the state.
func (a RawStateAccessor) Set(data []byte) error {
	if len(data) == 0 {
		return a.Clear()
	}
	return a.holder.Set(a.key, InternalState{Data: bytes.Clone(data)})
}

// Clear deletes the state of the key.
func (a RawStateAccessor) Clear() error {
	return a.holder.Clear(a.key)
}

// YSONState is mutable typed internal state for one key.
type YSONState[T any] struct {
	raw RawStateAccessor

	value   T
	exists  bool
	cleared bool
	mutable bool
	initial []byte
}

// OpenYSONState binds mutable internal state to the key of the input being handled.
func OpenYSONState[T any](rt Runtime, name string, input Input) (*YSONState[T], error) {
	return openTrackedState(rt, name, input, loadYSONState[T])
}

func loadYSONState[T any](raw RawStateAccessor) (*YSONState[T], error) {
	state := &YSONState[T]{raw: raw}
	data, ok := raw.Get()
	if !ok {
		return state, nil
	}
	if err := yson.Unmarshal(data, &state.value); err != nil {
		return nil, xerrors.Errorf("flow: state %q: %w", raw.holder.Name(), err)
	}
	state.exists = true
	state.initial = data
	return state, nil
}

// Empty reports whether the state has no value.
func (s *YSONState[T]) Empty() bool {
	return !s.exists || s.cleared
}

// Get returns the mutable state value, or false when the key has none.
func (s *YSONState[T]) Get() (*T, bool) {
	if s.Empty() {
		return nil, false
	}
	s.mutable = true
	return &s.value, true
}

// Value returns the mutable state value, creating its zero value when absent.
func (s *YSONState[T]) Value() *T {
	s.exists = true
	s.cleared = false
	s.mutable = true
	return &s.value
}

// Clear deletes the state of the key.
func (s *YSONState[T]) Clear() {
	s.cleared = true
	s.exists = false
	var zero T
	s.value = zero
}

// ReadOnly returns a view of the same value that is never written back.
func (s *YSONState[T]) ReadOnly() ReadOnlyYSONState[T] {
	return ReadOnlyYSONState[T]{state: s}
}

func (s *YSONState[T]) flush() error {
	if s.cleared {
		return clearTrackedState(s.raw, s.initial)
	}
	if !s.mutable {
		return nil
	}
	data, err := yson.MarshalFormat(s.value, yson.FormatBinary)
	if err != nil {
		return xerrors.Errorf("flow: state %q: %w", s.raw.holder.Name(), err)
	}
	return setTrackedState(s.raw, s.initial, data)
}

// ReadOnlyYSONState reads a YSON internal state without writing it back.
type ReadOnlyYSONState[T any] struct {
	state *YSONState[T]
}

// Empty reports whether the state has no value.
func (s ReadOnlyYSONState[T]) Empty() bool {
	return s.state.Empty()
}

// Get returns the state value, or false when the key has none.
func (s ReadOnlyYSONState[T]) Get() (*T, bool) {
	if s.state.Empty() {
		return nil, false
	}
	return &s.state.value, true
}

// Value returns the state value, or its zero value when absent; the state is not created.
func (s ReadOnlyYSONState[T]) Value() *T {
	return &s.state.value
}

// protoStateMarshalOptions keep the encoding stable across requests, so that a message
// nobody changed compares equal to the bytes it arrived with.
var protoStateMarshalOptions = proto.MarshalOptions{Deterministic: true}

// ProtoStateValue is a generated protobuf message pointer.
type ProtoStateValue[T any] interface {
	*T
	proto.Message
}

// ProtoState is mutable protobuf internal state for one key.
type ProtoState[T any, PT ProtoStateValue[T]] struct {
	raw RawStateAccessor

	value   PT
	cleared bool
	mutable bool
	initial []byte
}

// OpenProtoState binds mutable internal state to the key of the input being handled.
func OpenProtoState[T any, PT ProtoStateValue[T]](rt Runtime, name string, input Input) (*ProtoState[T, PT], error) {
	return openTrackedState(rt, name, input, loadProtoState[T, PT])
}

func loadProtoState[T any, PT ProtoStateValue[T]](raw RawStateAccessor) (*ProtoState[T, PT], error) {
	state := &ProtoState[T, PT]{raw: raw}
	data, ok := raw.Get()
	if !ok {
		return state, nil
	}
	value := PT(new(T))
	if err := proto.Unmarshal(data, value); err != nil {
		return nil, xerrors.Errorf("flow: state %q: %w", raw.holder.Name(), err)
	}
	state.value = value
	state.initial = data
	return state, nil
}

// Empty reports whether the state has no value.
func (s *ProtoState[T, PT]) Empty() bool {
	return s.value == nil || s.cleared
}

// Get returns the mutable state message, or false when the key has none.
func (s *ProtoState[T, PT]) Get() (PT, bool) {
	if s.Empty() {
		return nil, false
	}
	s.mutable = true
	return s.value, true
}

// Or returns the mutable state message, storing fallback as the value when the key has none.
func (s *ProtoState[T, PT]) Or(fallback PT) PT {
	if value, ok := s.Get(); ok {
		return value
	}
	s.bind(fallback)
	return fallback
}

// Set stores value as the state of the key; a message that encodes to no bytes deletes the state.
func (s *ProtoState[T, PT]) Set(value PT) error {
	// Report an unencodable message here, where the caller can still see which write caused it.
	if _, err := protoStateMarshalOptions.Marshal(value); err != nil {
		return xerrors.Errorf("flow: state %q: %w", s.raw.holder.Name(), err)
	}
	s.bind(value)
	return nil
}

// Clear deletes the state of the key.
func (s *ProtoState[T, PT]) Clear() {
	s.cleared = true
	s.value = nil
}

// ReadOnly returns a view of the same message that is never written back.
func (s *ProtoState[T, PT]) ReadOnly() ReadOnlyProtoState[T, PT] {
	return ReadOnlyProtoState[T, PT]{state: s}
}

func (s *ProtoState[T, PT]) bind(value PT) {
	s.value = value
	s.cleared = false
	s.mutable = true
}

func (s *ProtoState[T, PT]) flush() error {
	if s.cleared {
		return clearTrackedState(s.raw, s.initial)
	}
	if !s.mutable {
		return nil
	}
	data, err := protoStateMarshalOptions.Marshal(s.value)
	if err != nil {
		return xerrors.Errorf("flow: state %q: %w", s.raw.holder.Name(), err)
	}
	// A message with no field set encodes to no bytes, which is how an absent state is spelled.
	if len(data) == 0 {
		return clearTrackedState(s.raw, s.initial)
	}
	return setTrackedState(s.raw, s.initial, data)
}

// ReadOnlyProtoState reads a protobuf internal state without writing it back.
type ReadOnlyProtoState[T any, PT ProtoStateValue[T]] struct {
	state *ProtoState[T, PT]
}

// Empty reports whether the state has no value.
func (s ReadOnlyProtoState[T, PT]) Empty() bool {
	return s.state.Empty()
}

// Get returns the state message, or false when the key has none.
func (s ReadOnlyProtoState[T, PT]) Get() (PT, bool) {
	if s.state.Empty() {
		return nil, false
	}
	return s.state.value, true
}

// Or returns the state message, or fallback when the key has none; the state is not created.
func (s ReadOnlyProtoState[T, PT]) Or(fallback PT) PT {
	if value, ok := s.Get(); ok {
		return value
	}
	return fallback
}

// ExternalStateAccessor reads and writes an owned external state row.
type ExternalStateAccessor struct {
	holder *StatesHolder[ExternalState]
	key    Payload
}

// OpenExternalState binds an owned external state to an input key.
func OpenExternalState(rt Runtime, name string, input Input) (ExternalStateAccessor, error) {
	holder, err := rt.ExternalState(name)
	if err != nil {
		return ExternalStateAccessor{}, err
	}
	return ExternalStateAccessor{holder: holder, key: input.PartitionKey()}, nil
}

// Schema returns the schema of the state table rows.
func (a ExternalStateAccessor) Schema() Schema {
	return a.holder.StateSchema()
}

// Get returns the row stored for the key.
func (a ExternalStateAccessor) Get() (Payload, bool) {
	state, ok := a.holder.Get(a.key)
	if !ok || state.Reset || state.Value.row == nil {
		return Payload{}, false
	}
	return state.Value, true
}

// Or returns the row stored for the key, or fallback if it has none.
func (a ExternalStateAccessor) Or(fallback Payload) Payload {
	if row, ok := a.Get(); ok {
		return row
	}
	return fallback
}

// ConvertTo decodes the stored row into value.
func (a ExternalStateAccessor) ConvertTo(value any) (bool, error) {
	row, ok := a.Get()
	if !ok {
		return false, nil
	}
	if err := row.ConvertTo(value); err != nil {
		return false, xerrors.Errorf("flow: state %q: %w", a.holder.Name(), err)
	}
	return true, nil
}

// ConvertFrom encodes value and stores it as the row of the key.
func (a ExternalStateAccessor) ConvertFrom(value any) error {
	row, err := a.Builder().SetStruct(value).Finish()
	if err != nil {
		return xerrors.Errorf("flow: state %q: %w", a.holder.Name(), err)
	}
	return a.Set(row)
}

// Builder returns a builder initialized with the stored row.
func (a ExternalStateAccessor) Builder() *PayloadBuilder {
	if row, ok := a.Get(); ok {
		return row.ToBuilder()
	}
	return NewPayloadBuilder(a.holder.StateSchema())
}

// Set stores value as the row of the key; the zero Payload deletes the row.
func (a ExternalStateAccessor) Set(value Payload) error {
	if value.row == nil {
		return a.Clear()
	}
	return a.holder.Set(a.key, ExternalState{Value: value})
}

// Clear deletes the row of the key.
func (a ExternalStateAccessor) Clear() error {
	return a.holder.Clear(a.key)
}

// JoinedExternalStateAccessor reads a joined external state row.
type JoinedExternalStateAccessor struct {
	state ExternalStateAccessor
}

// OpenJoinedExternalState binds a joined external state to an input key.
func OpenJoinedExternalState(rt Runtime, name string, input Input) (JoinedExternalStateAccessor, error) {
	holder, err := rt.JoinedExternalState(name)
	if err != nil {
		return JoinedExternalStateAccessor{}, err
	}
	return JoinedExternalStateAccessor{
		state: ExternalStateAccessor{holder: holder, key: input.PartitionKey()},
	}, nil
}

// Schema returns the schema of the joined state table rows.
func (a JoinedExternalStateAccessor) Schema() Schema {
	return a.state.Schema()
}

// Get returns the row joined for the key.
func (a JoinedExternalStateAccessor) Get() (Payload, bool) {
	return a.state.Get()
}

// Or returns the row joined for the key, or fallback if none was joined.
func (a JoinedExternalStateAccessor) Or(fallback Payload) Payload {
	return a.state.Or(fallback)
}

// ConvertTo decodes the joined row into value.
func (a JoinedExternalStateAccessor) ConvertTo(value any) (bool, error) {
	return a.state.ConvertTo(value)
}
