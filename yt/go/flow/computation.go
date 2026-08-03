package flow

import (
	"context"
	"fmt"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

// ErrDistributeOnTransform reports source-only delivery control used by a transform.
var ErrDistributeOnTransform = xerrors.NewSentinel("distribute flag is valid for source computations only")

type computationType int

const (
	computationTypeSource    computationType = 0
	computationTypeTransform computationType = 1
)

func (t computationType) String() string {
	switch t {
	case computationTypeSource:
		return "Source"
	case computationTypeTransform:
		return "Transform"
	default:
		return fmt.Sprintf("computationType(%d)", int(t))
	}
}

// Runtime exposes per-request state and configuration.
type Runtime interface {
	// Parameters returns the computation parameters from the static spec.
	Parameters() Parameters

	// DynamicParameters returns the computation parameters from the dynamic spec.
	DynamicParameters() Parameters

	// KeySchema returns the schema of the key the batch is grouped by.
	KeySchema() Schema

	// StreamSpecs resolves the streams of the computation.
	StreamSpecs() StreamSpecs

	// MessageBuilder returns a builder for messages of an output stream.
	MessageBuilder(streamID string) (*MessageBuilder, error)

	// MinWatermark returns the lowest event watermark across the input streams.
	MinWatermark() uint64

	// Watermark returns the event watermark of one input stream.
	Watermark(streamID string) (uint64, bool)

	// InternalState returns the holder of a state declared in parameters.internal_states.
	InternalState(name string) (*StatesHolder[InternalState], error)

	// ExternalState returns the holder of a state declared in external_state_managers.
	ExternalState(name string) (*StatesHolder[ExternalState], error)

	// JoinedExternalState returns a joined external state.
	JoinedExternalState(name string) (*StatesHolder[ExternalState], error)

	getYSONState(ysonStateKey, func() (trackedYSONState, error)) (trackedYSONState, error)
	resetYSONStates()
	flushYSONStates() error
}

// RowFunction handles messages one at a time.
type RowFunction interface {
	OnMessage(ctx context.Context, rt Runtime, msg ExtendedMessage, out OutputCollector) error
}

// RowTimerFunction handles a fired timer.
type RowTimerFunction interface {
	OnTimer(ctx context.Context, rt Runtime, timer Timer, out OutputCollector) error
}

// RowVisitFunction handles a visit of a key emitted by a key-visitor stream.
type RowVisitFunction interface {
	OnVisit(ctx context.Context, rt Runtime, visit Visit, out OutputCollector) error
}

// BatchFunction handles a batch of messages.
type BatchFunction interface {
	OnMessages(ctx context.Context, rt Runtime, msgs []ExtendedMessage, out OutputCollector) error
}

// BatchTimerFunction handles the timers of a batch.
type BatchTimerFunction interface {
	OnTimers(ctx context.Context, rt Runtime, timers []Timer, out OutputCollector) error
}

// BatchVisitFunction handles the visits of a batch.
type BatchVisitFunction interface {
	OnVisits(ctx context.Context, rt Runtime, visits []Visit, out OutputCollector) error
}

// RowFunc adapts a plain function to RowFunction.
type RowFunc func(ctx context.Context, rt Runtime, msg ExtendedMessage, out OutputCollector) error

func (f RowFunc) OnMessage(ctx context.Context, rt Runtime, msg ExtendedMessage, out OutputCollector) error {
	return f(ctx, rt, msg, out)
}

// BatchFunc adapts a plain function to BatchFunction.
type BatchFunc func(ctx context.Context, rt Runtime, msgs []ExtendedMessage, out OutputCollector) error

func (f BatchFunc) OnMessages(ctx context.Context, rt Runtime, msgs []ExtendedMessage, out OutputCollector) error {
	return f(ctx, rt, msgs, out)
}

// Batch is the input of one ProcessBatch call.
type Batch struct {
	Messages []ExtendedMessage
	Timers   []Timer
	Visits   []Visit
}

// Computation binds an id to a function. Functions may be called concurrently.
type Computation struct {
	id    string
	typ   computationType
	row   RowFunction
	batch BatchFunction
}

// NewRowComputation binds a per-message function to a transform computation.
func NewRowComputation(id string, fn RowFunction) *Computation {
	return newRowComputation(id, computationTypeTransform, fn)
}

// NewBatchComputation binds a per-batch function to a transform computation.
func NewBatchComputation(id string, fn BatchFunction) *Computation {
	return newBatchComputation(id, computationTypeTransform, fn)
}

// NewRowSourceComputation binds a per-message source function.
func NewRowSourceComputation(id string, fn RowFunction) *Computation {
	return newRowComputation(id, computationTypeSource, fn)
}

// NewBatchSourceComputation binds a per-batch function to a source computation.
func NewBatchSourceComputation(id string, fn BatchFunction) *Computation {
	return newBatchComputation(id, computationTypeSource, fn)
}

func newRowComputation(id string, typ computationType, fn RowFunction) *Computation {
	if fn == nil {
		panic(fmt.Sprintf("flow: computation %q: row function is nil", id))
	}
	return &Computation{id: id, typ: typ, row: fn}
}

func newBatchComputation(id string, typ computationType, fn BatchFunction) *Computation {
	if fn == nil {
		panic(fmt.Sprintf("flow: computation %q: batch function is nil", id))
	}
	return &Computation{id: id, typ: typ, batch: fn}
}

// ID returns the computation id the worker addresses this computation by.
func (c *Computation) ID() string {
	return c.id
}

// Process runs one batch and returns its output groups.
func (c *Computation) Process(ctx context.Context, rt Runtime, batch Batch) ([]OutputGroup, error) {
	root := newRootCollector()

	var err error
	if c.row != nil {
		err = c.processRows(ctx, rt, batch, root)
	} else {
		err = c.processBatch(ctx, rt, batch, root)
	}
	if err != nil {
		rt.resetYSONStates()
		return nil, err
	}

	results := root.CollectResults()
	if c.typ == computationTypeTransform {
		for _, result := range results {
			if len(result.Distribute) != 0 {
				rt.resetYSONStates()
				return nil, xerrors.Errorf("computation %q: %w", c.id, ErrDistributeOnTransform)
			}
		}
	}

	if err := rt.flushYSONStates(); err != nil {
		return nil, xerrors.Errorf("computation %q: flush YSON state: %w", c.id, err)
	}

	return results, nil
}

func (c *Computation) processRows(ctx context.Context, rt Runtime, batch Batch, root *rootCollector) error {
	for _, msg := range batch.Messages {
		if err := c.row.OnMessage(ctx, rt, msg, root.WithParentIDs(msg.ID)); err != nil {
			return c.inputError("OnMessage", msg.ID, err)
		}
	}

	if fn, ok := c.row.(RowTimerFunction); ok {
		for _, timer := range batch.Timers {
			if err := fn.OnTimer(ctx, rt, timer, root.WithParentIDs(timer.ID)); err != nil {
				return c.inputError("OnTimer", timer.ID, err)
			}
		}
	}

	if fn, ok := c.row.(RowVisitFunction); ok {
		for _, visit := range batch.Visits {
			if err := fn.OnVisit(ctx, rt, visit, root.WithParentIDs(visit.ID)); err != nil {
				return c.inputError("OnVisit", visit.ID, err)
			}
		}
	}

	return nil
}

func (c *Computation) processBatch(ctx context.Context, rt Runtime, batch Batch, root *rootCollector) error {
	if len(batch.Messages) > 0 {
		out := root.WithParentIDs(inputIDs(batch.Messages, func(m ExtendedMessage) string { return m.ID })...)
		if err := c.batch.OnMessages(ctx, rt, batch.Messages, out); err != nil {
			return c.handlerError("OnMessages", err)
		}
	}

	if fn, ok := c.batch.(BatchTimerFunction); ok && len(batch.Timers) > 0 {
		out := root.WithParentIDs(inputIDs(batch.Timers, func(t Timer) string { return t.ID })...)
		if err := fn.OnTimers(ctx, rt, batch.Timers, out); err != nil {
			return c.handlerError("OnTimers", err)
		}
	}

	if fn, ok := c.batch.(BatchVisitFunction); ok && len(batch.Visits) > 0 {
		out := root.WithParentIDs(inputIDs(batch.Visits, func(v Visit) string { return v.ID })...)
		if err := fn.OnVisits(ctx, rt, batch.Visits, out); err != nil {
			return c.handlerError("OnVisits", err)
		}
	}

	return nil
}

func (c *Computation) inputError(handler, inputID string, err error) error {
	return xerrors.Errorf("computation %q: %s on input %q: %w", c.id, handler, inputID, err)
}

func (c *Computation) handlerError(handler string, err error) error {
	return xerrors.Errorf("computation %q: %s: %w", c.id, handler, err)
}

func inputIDs[T any](inputs []T, id func(T) string) []string {
	ids := make([]string, len(inputs))
	for i, input := range inputs {
		ids[i] = id(input)
	}
	return ids
}
