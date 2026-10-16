package flow

import (
	"iter"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/wire"
)

var (
	// ErrNoStateSchema reports an external state declared without the schema of its rows.
	ErrNoStateSchema = xerrors.NewSentinel("external state has no schema")

	// ErrStateNotRead reports an external state absent from the request.
	ErrStateNotRead = xerrors.NewSentinel("external state was not read by this request")
)

// StateValue is a value stored by StatesHolder.
type StateValue[T any] interface {
	Cleared() T
}

// InternalState is an entry of a keyed internal state.
type InternalState struct {
	// Reset asks the worker to drop the state stored for the key; Data is then ignored.
	Reset bool
	Data  []byte
}

// Cleared returns the entry that deletes the state stored for a key.
func (s InternalState) Cleared() InternalState {
	return InternalState{Reset: true}
}

// ExternalState is an entry of a keyed external state.
type ExternalState struct {
	// Reset asks the worker to delete the row stored for the key; Value is then ignored.
	Reset bool
	Value Payload
}

// Cleared returns the entry that deletes the row stored for a key.
func (s ExternalState) Cleared() ExternalState {
	return ExternalState{Reset: true}
}

type stateEntry[T any] struct {
	key      Payload
	value    T
	modified bool
}

// StatesHolder holds one keyed state for a request.
// Modified returns only entries changed after loading.
type StatesHolder[T StateValue[T]] struct {
	name        string
	stateSchema Schema

	index   map[string]int
	entries []stateEntry[T]

	modified int
}

func newInternalStatesHolder(name string) *StatesHolder[InternalState] {
	return &StatesHolder[InternalState]{name: name, index: map[string]int{}}
}

func newExternalStatesHolder(name string, stateSchema Schema) (*StatesHolder[ExternalState], error) {
	if stateSchema.Len() == 0 {
		return nil, xerrors.Errorf("flow: %w: state %q", ErrNoStateSchema, name)
	}
	return &StatesHolder[ExternalState]{
		name:        name,
		stateSchema: stateSchema,
		index:       map[string]int{},
	}, nil
}

// Name returns the state name this holder stores entries of.
func (h *StatesHolder[T]) Name() string {
	return h.name
}

// StateSchema returns the external state row schema.
func (h *StatesHolder[T]) StateSchema() Schema {
	return h.stateSchema
}

// Len returns the number of entries.
func (h *StatesHolder[T]) Len() int {
	return len(h.entries)
}

// Get returns the entry stored for key.
func (h *StatesHolder[T]) Get(key Payload) (T, bool) {
	var zero T
	rowKey, err := encodeStateKey(key)
	if err != nil {
		return zero, false
	}
	i, ok := h.index[rowKey]
	if !ok {
		return zero, false
	}
	return h.entries[i].value, true
}

// Load stores an entry without marking it modified.
func (h *StatesHolder[T]) Load(key Payload, value T) error {
	return h.put(key, value, false)
}

// Set stores a user write for key and marks it modified.
func (h *StatesHolder[T]) Set(key Payload, value T) error {
	return h.put(key, value, true)
}

// Clear records the deletion of the state stored for key.
func (h *StatesHolder[T]) Clear(key Payload) error {
	var zero T
	return h.Set(key, zero.Cleared())
}

// HasModified reports whether any entry was written during this request.
func (h *StatesHolder[T]) HasModified() bool {
	return h.modified > 0
}

// All iterates over every entry held, in the order the keys were first seen.
func (h *StatesHolder[T]) All() iter.Seq2[Payload, T] {
	return func(yield func(Payload, T) bool) {
		for i := range h.entries {
			if !yield(h.entries[i].key, h.entries[i].value) {
				return
			}
		}
	}
}

// Modified iterates over modified entries in insertion order.
func (h *StatesHolder[T]) Modified() iter.Seq2[Payload, T] {
	return func(yield func(Payload, T) bool) {
		for i := range h.entries {
			if !h.entries[i].modified {
				continue
			}
			if !yield(h.entries[i].key, h.entries[i].value) {
				return
			}
		}
	}
}

func (h *StatesHolder[T]) put(key Payload, value T, modified bool) error {
	rowKey, err := encodeStateKey(key)
	if err != nil {
		return xerrors.Errorf("flow: state %q: %w", h.name, err)
	}

	if i, ok := h.index[rowKey]; ok {
		h.entries[i].value = value
		if modified && !h.entries[i].modified {
			h.entries[i].modified = true
			h.modified++
		}
		return nil
	}

	h.index[rowKey] = len(h.entries)
	h.entries = append(h.entries, stateEntry[T]{key: key, value: value, modified: modified})
	if modified {
		h.modified++
	}
	return nil
}

func encodeStateKey(key Payload) (string, error) {
	encoded, err := wire.MarshalRowProto(key.row)
	if err != nil {
		return "", xerrors.Errorf("unusable state key: %w", err)
	}
	return string(encoded), nil
}
