package flow

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var errHandler = errors.New("handler failed")

type stubRuntime struct {
	Runtime
}

func (r *stubRuntime) getYSONState(
	key ysonStateKey,
	create func() (trackedYSONState, error),
) (trackedYSONState, error) {
	return create()
}

func (r *stubRuntime) resetYSONStates() {}

func (r *stubRuntime) flushYSONStates() error { return nil }

func testInput(id string) ExtendedMessage {
	return ExtendedMessage{Message: Message{Meta: Meta{ID: id, StreamID: "in"}}}
}

func testTimer(id string) Timer {
	return Timer{Meta: Meta{ID: id, StreamID: "timers"}, TriggerTimestamp: 100}
}

func testVisit(id string) Visit {
	return NewVisit(Meta{ID: id}, Payload{})
}

func testBatch(messages, timers, visits []string) Batch {
	return Batch{
		Messages: mapSlice(messages, testInput),
		Timers:   mapSlice(timers, testTimer),
		Visits:   mapSlice(visits, testVisit),
	}
}

func groupParents(groups []OutputGroup) [][]string {
	return mapSlice(groups, func(g OutputGroup) []string { return g.ParentIDs })
}

func mapSlice[T, R any](in []T, f func(T) R) []R {
	if in == nil {
		return nil
	}
	out := make([]R, len(in))
	for i, v := range in {
		out[i] = f(v)
	}
	return out
}

type rowRecorder struct {
	messages []string
	timers   []string
	visits   []string
	contexts []Runtime
	failOn   string
}

func (f *rowRecorder) OnMessage(_ context.Context, rt Runtime, msg ExtendedMessage, out OutputCollector) error {
	f.messages = append(f.messages, msg.ID)
	f.contexts = append(f.contexts, rt)
	return f.emit(msg.ID, out)
}

func (f *rowRecorder) OnTimer(_ context.Context, _ Runtime, timer Timer, out OutputCollector) error {
	f.timers = append(f.timers, timer.ID)
	return f.emit(timer.ID, out)
}

func (f *rowRecorder) OnVisit(_ context.Context, _ Runtime, visit Visit, out OutputCollector) error {
	f.visits = append(f.visits, visit.ID)
	return f.emit(visit.ID, out)
}

func (f *rowRecorder) emit(inputID string, out OutputCollector) error {
	if inputID == f.failOn {
		return errHandler
	}
	out.AddMessage(testMessage("out-" + inputID))
	return nil
}

type batchRecorder struct {
	messageBatches [][]string
	timerBatches   [][]string
	visitBatches   [][]string
	failOn         string
}

func (f *batchRecorder) OnMessages(_ context.Context, _ Runtime, msgs []ExtendedMessage, out OutputCollector) error {
	ids := inputIDs(msgs, func(m ExtendedMessage) string { return m.ID })
	f.messageBatches = append(f.messageBatches, ids)
	return f.emit("OnMessages", out)
}

func (f *batchRecorder) OnTimers(_ context.Context, _ Runtime, timers []Timer, out OutputCollector) error {
	f.timerBatches = append(f.timerBatches, inputIDs(timers, func(t Timer) string { return t.ID }))
	return f.emit("OnTimers", out)
}

func (f *batchRecorder) OnVisits(_ context.Context, _ Runtime, visits []Visit, out OutputCollector) error {
	f.visitBatches = append(f.visitBatches, inputIDs(visits, func(v Visit) string { return v.ID }))
	return f.emit("OnVisits", out)
}

func (f *batchRecorder) emit(handler string, out OutputCollector) error {
	if handler == f.failOn {
		return errHandler
	}
	out.AddMessage(testMessage("out-" + handler))
	return nil
}

func TestRowComputationOpensGroupPerMessage(t *testing.T) {
	fn := &rowRecorder{}
	computation := NewRowComputation("counter", fn)

	groups, err := computation.Process(context.Background(), &stubRuntime{}, testBatch([]string{"m1", "m2"}, nil, nil))
	require.NoError(t, err)

	require.Equal(t, []string{"m1", "m2"}, fn.messages)
	require.Equal(t, [][]string{{"m1"}, {"m2"}}, groupParents(groups))
	require.Equal(t, []string{"out-m1"}, messageIDs(groups[0].Messages))
	require.Equal(t, []string{"out-m2"}, messageIDs(groups[1].Messages))
}

func TestRowComputationOpensGroupPerTimerAndVisit(t *testing.T) {
	fn := &rowRecorder{}
	computation := NewRowComputation("counter", fn)

	groups, err := computation.Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1"}, []string{"t1", "t2"}, []string{"v1"}),
	)
	require.NoError(t, err)

	require.Equal(t, []string{"t1", "t2"}, fn.timers)
	require.Equal(t, []string{"v1"}, fn.visits)
	require.Equal(t, [][]string{{"m1"}, {"t1"}, {"t2"}, {"v1"}}, groupParents(groups))
}

func TestRowComputationSkipsUnhandledTimersAndVisits(t *testing.T) {
	var handled []string
	fn := RowFunc(func(_ context.Context, _ Runtime, msg ExtendedMessage, out OutputCollector) error {
		handled = append(handled, msg.ID)
		out.AddMessage(testMessage("out-" + msg.ID))
		return nil
	})

	groups, err := NewRowComputation("counter", fn).Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1"}, []string{"t1"}, []string{"v1"}),
	)
	require.NoError(t, err)

	require.Equal(t, []string{"m1"}, handled)
	require.Equal(t, [][]string{{"m1"}}, groupParents(groups))
}

func TestBatchComputationOpensOneGroupForAllMessages(t *testing.T) {
	fn := &batchRecorder{}
	computation := NewBatchComputation("aggregator", fn)

	groups, err := computation.Process(context.Background(), &stubRuntime{}, testBatch([]string{"m1", "m2", "m3"}, nil, nil))
	require.NoError(t, err)

	require.Equal(t, [][]string{{"m1", "m2", "m3"}}, fn.messageBatches)
	require.Equal(t, [][]string{{"m1", "m2", "m3"}}, groupParents(groups))
	require.Equal(t, []string{"out-OnMessages"}, messageIDs(groups[0].Messages))
}

func TestBatchComputationSeparatesMessagesTimersAndVisits(t *testing.T) {
	fn := &batchRecorder{}
	computation := NewBatchComputation("aggregator", fn)

	groups, err := computation.Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1", "m2"}, []string{"t1", "t2"}, []string{"v1"}),
	)
	require.NoError(t, err)

	require.Equal(t, [][]string{{"t1", "t2"}}, fn.timerBatches)
	require.Equal(t, [][]string{{"v1"}}, fn.visitBatches)
	require.Equal(t, [][]string{{"m1", "m2"}, {"t1", "t2"}, {"v1"}}, groupParents(groups))
}

func TestBatchComputationSkipsEmptyInputKinds(t *testing.T) {
	fn := &batchRecorder{}
	computation := NewBatchComputation("aggregator", fn)

	groups, err := computation.Process(context.Background(), &stubRuntime{}, testBatch(nil, []string{"t1"}, nil))
	require.NoError(t, err)

	require.Empty(t, fn.messageBatches)
	require.Empty(t, fn.visitBatches)
	require.Equal(t, [][]string{{"t1"}}, groupParents(groups))
}

func TestBatchComputationSkipsUnhandledTimersAndVisits(t *testing.T) {
	var handled [][]string
	fn := BatchFunc(func(_ context.Context, _ Runtime, msgs []ExtendedMessage, out OutputCollector) error {
		handled = append(handled, inputIDs(msgs, func(m ExtendedMessage) string { return m.ID }))
		out.AddMessage(testMessage("out"))
		return nil
	})

	groups, err := NewBatchComputation("aggregator", fn).Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1"}, []string{"t1"}, []string{"v1"}),
	)
	require.NoError(t, err)

	require.Equal(t, [][]string{{"m1"}}, handled)
	require.Equal(t, [][]string{{"m1"}}, groupParents(groups))
}

func TestProcessDropsGroupsWithoutOutput(t *testing.T) {
	fn := RowFunc(func(_ context.Context, _ Runtime, msg ExtendedMessage, out OutputCollector) error {
		if msg.ID == "m2" {
			out.AddMessage(testMessage("out-m2"))
		}
		return nil
	})

	groups, err := NewRowComputation("counter", fn).Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1", "m2", "m3"}, nil, nil),
	)
	require.NoError(t, err)

	require.Equal(t, [][]string{{"m2"}}, groupParents(groups))
}

func TestRowComputationAbandonsBatchOnHandlerError(t *testing.T) {
	fn := &rowRecorder{failOn: "m2"}

	groups, err := NewRowComputation("counter", fn).Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1", "m2", "m3"}, nil, nil),
	)
	require.ErrorIs(t, err, errHandler)
	require.Nil(t, groups)
	require.Equal(t, []string{"m1", "m2"}, fn.messages)
	require.Contains(t, err.Error(), "counter")
	require.Contains(t, err.Error(), "m2")
}

func TestRowComputationReportsFailingHandler(t *testing.T) {
	fn := &rowRecorder{failOn: "t1"}

	_, err := NewRowComputation("counter", fn).Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1"}, []string{"t1"}, nil),
	)
	require.ErrorIs(t, err, errHandler)
	require.Contains(t, err.Error(), "OnTimer")
}

func TestBatchComputationAbandonsBatchOnHandlerError(t *testing.T) {
	fn := &batchRecorder{failOn: "OnMessages"}

	groups, err := NewBatchComputation("aggregator", fn).Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1"}, []string{"t1"}, nil),
	)
	require.ErrorIs(t, err, errHandler)
	require.Nil(t, groups)
	require.Empty(t, fn.timerBatches)
	require.Contains(t, err.Error(), "OnMessages")
}

func TestProcessPassesRuntimeContextThrough(t *testing.T) {
	rt := &stubRuntime{}
	fn := &rowRecorder{}

	_, err := NewRowComputation("counter", fn).Process(context.Background(), rt, testBatch([]string{"m1", "m2"}, nil, nil))
	require.NoError(t, err)

	require.Equal(t, []Runtime{rt, rt}, fn.contexts)
}

func TestProcessPassesRequestContextThrough(t *testing.T) {
	type requestKey struct{}
	ctx := context.WithValue(context.Background(), requestKey{}, "batch")

	var seen []any
	fn := RowFunc(func(ctx context.Context, _ Runtime, _ ExtendedMessage, _ OutputCollector) error {
		seen = append(seen, ctx.Value(requestKey{}))
		return nil
	})

	_, err := NewRowComputation("counter", fn).Process(ctx, &stubRuntime{}, testBatch([]string{"m1", "m2"}, nil, nil))
	require.NoError(t, err)

	require.Equal(t, []any{"batch", "batch"}, seen)
}

func TestComputationRunsConcurrentlyOnOneFunction(t *testing.T) {
	const goroutines = 4

	entered := make(chan struct{}, goroutines)
	release := make(chan struct{})
	fn := RowFunc(func(_ context.Context, _ Runtime, msg ExtendedMessage, out OutputCollector) error {
		entered <- struct{}{}
		<-release
		out.AddMessage(testMessage("out-" + msg.ID))
		return nil
	})
	computation := NewRowComputation("counter", fn)

	done := make(chan []OutputGroup, goroutines)
	for i := range goroutines {
		go func() {
			groups, err := computation.Process(
				context.Background(),
				&stubRuntime{},
				testBatch([]string{"m" + strconv.Itoa(i)}, nil, nil),
			)
			if err != nil {
				done <- nil
				return
			}
			done <- groups
		}()
	}

	for range goroutines {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("handlers did not run concurrently")
		}
	}
	close(release)

	handled := make([]string, 0, goroutines)
	for range goroutines {
		groups := <-done
		require.Len(t, groups, 1)
		handled = append(handled, groups[0].ParentIDs...)
	}
	require.ElementsMatch(t, []string{"m0", "m1", "m2", "m3"}, handled)
}

func TestComputationConstructorsRejectNilFunction(t *testing.T) {
	require.PanicsWithValue(t, `flow: computation "counter": row function is nil`, func() {
		NewRowComputation("counter", nil)
	})
	require.PanicsWithValue(t, `flow: computation "aggregator": batch function is nil`, func() {
		NewBatchComputation("aggregator", nil)
	})
	require.Panics(t, func() { NewRowSourceComputation("reader", nil) })
	require.Panics(t, func() { NewBatchSourceComputation("ingest", nil) })
}

func TestProcessKeepsDistributeAlignedWithMessages(t *testing.T) {
	fn := RowFunc(func(_ context.Context, _ Runtime, msg ExtendedMessage, out OutputCollector) error {
		out.AddMessage(testMessage("kept-" + msg.ID))
		out.AddUndistributedMessage(testMessage("dropped-" + msg.ID))
		return nil
	})

	groups, err := NewRowSourceComputation("filter", fn).Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1"}, nil, nil),
	)
	require.NoError(t, err)

	require.Equal(t, []string{"kept-m1", "dropped-m1"}, messageIDs(groups[0].Messages))
	require.Equal(t, []bool{true, false}, groups[0].Distribute)
}

func TestTransformRejectsUndistributedMessages(t *testing.T) {
	fn := RowFunc(func(_ context.Context, _ Runtime, msg ExtendedMessage, out OutputCollector) error {
		out.AddUndistributedMessage(testMessage("dropped-" + msg.ID))
		return nil
	})

	_, err := NewRowComputation("filter", fn).Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1"}, nil, nil),
	)
	require.ErrorIs(t, err, ErrDistributeOnTransform)
}

func TestProcessCollectsTimers(t *testing.T) {
	fn := RowFunc(func(_ context.Context, _ Runtime, msg ExtendedMessage, out OutputCollector) error {
		out.AddTimer(TimerRequest{TriggerTimestamp: msg.EventTimestamp + 1000})
		return nil
	})

	groups, err := NewRowComputation("delayer", fn).Process(context.Background(), &stubRuntime{}, testBatch([]string{"m1"}, nil, nil))
	require.NoError(t, err)

	require.Equal(t, [][]string{{"m1"}}, groupParents(groups))
	require.Empty(t, groups[0].Messages)
	require.Equal(t, []TimerRequest{{TriggerTimestamp: 1000}}, groups[0].Timers)
}

func TestComputationTypes(t *testing.T) {
	fn := RowFunc(func(context.Context, Runtime, ExtendedMessage, OutputCollector) error { return nil })
	batchFn := BatchFunc(func(context.Context, Runtime, []ExtendedMessage, OutputCollector) error { return nil })

	require.Equal(t, computationTypeTransform, NewRowComputation("a", fn).typ)
	require.Equal(t, computationTypeTransform, NewBatchComputation("b", batchFn).typ)
	require.Equal(t, computationTypeSource, NewRowSourceComputation("c", fn).typ)
	require.Equal(t, computationTypeSource, NewBatchSourceComputation("d", batchFn).typ)

	require.Equal(t, "Source", computationTypeSource.String())
	require.Equal(t, "Transform", computationTypeTransform.String())
	require.Equal(t, "a", NewRowComputation("a", fn).ID())
}

func TestSourceComputationDispatchesLikeTransform(t *testing.T) {
	fn := &rowRecorder{}

	groups, err := NewRowSourceComputation("reader", fn).Process(
		context.Background(),
		&stubRuntime{},
		testBatch([]string{"m1"}, []string{"t1"}, nil),
	)
	require.NoError(t, err)

	require.Equal(t, [][]string{{"m1"}, {"t1"}}, groupParents(groups))
}
