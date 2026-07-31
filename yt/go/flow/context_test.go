package flow

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/proto/flow/companion"
)

type windowState struct {
	Count int64  `yson:"count"`
	Label string `yson:"label"`
}

func testRuntime(t *testing.T) *RequestRuntime {
	t.Helper()
	return NewRequestRuntime(testJob(t))
}

func keyedInput(t *testing.T, region string) ExtendedMessage {
	t.Helper()
	return ExtendedMessage{Key: stateKey(t, 1, region)}
}

func modifiedStateNames[T StateValue[T]](holders iter.Seq[*StatesHolder[T]]) []string {
	var names []string
	for holder := range holders {
		names = append(names, holder.Name())
	}
	return names
}

func TestRuntimeExposesJobConfiguration(t *testing.T) {
	r := testRuntime(t)

	var windowSize int
	require.NoError(t, r.Parameters().Get("window_size", &windowSize))
	require.Equal(t, 100, windowSize)

	require.NoError(t, r.DynamicParameters().Get("window_size", &windowSize))
	require.Equal(t, 200, windowSize)

	require.Equal(t, 2, r.KeySchema().Len())
	name, ok := r.KeySchema().ColumnName(1)
	require.True(t, ok)
	require.Equal(t, "user_id", name)

	stream, ok := r.StreamSpecs().Stream("clicks")
	require.True(t, ok)
	require.Equal(t, "clicks", stream.ID)
}

func TestRuntimeMessageBuilderIsTypedByItsStream(t *testing.T) {
	r := testRuntime(t)

	b, err := r.MessageBuilder("clicks")
	require.NoError(t, err)

	msg, err := b.Set("url", "https://ya.ru").Finish()
	require.NoError(t, err)
	require.Equal(t, "clicks", msg.StreamID)
	url, err := msg.Payload.String("url")
	require.NoError(t, err)
	require.Equal(t, "https://ya.ru", url)

	_, err = b.Set("url", "https://ya.ru").Set("referer", "x").Finish()
	require.ErrorIs(t, err, ErrColumnNotFound)
}

func TestRuntimeRejectsUnknownOutputStream(t *testing.T) {
	r := testRuntime(t)

	_, err := r.MessageBuilder("shows")
	require.ErrorIs(t, err, ErrUnknownStream)
}

func TestRuntimeStreamSpecsOverrideReplacesJobStreams(t *testing.T) {
	r := testRuntime(t)

	r.SetStreamSpecs(NewStreamSpecs(
		map[string]int64{"ingest": 42},
		[]Stream{NewStream("ingest", streamSchema("raw"))},
	))

	specID, ok := r.StreamSpecs().SpecID("ingest")
	require.True(t, ok)
	require.Equal(t, int64(42), specID)

	b, err := r.MessageBuilder("ingest")
	require.NoError(t, err)
	_, err = b.Set("raw", "payload").Finish()
	require.NoError(t, err)

	_, err = r.MessageBuilder("clicks")
	require.ErrorIs(t, err, ErrUnknownStream)
}

func TestRuntimeWatermarks(t *testing.T) {
	r := testRuntime(t)

	r.SetWatermark("clicks", 300)
	r.SetWatermark("shows", 100)
	r.SetWatermark("timers", 200)

	watermark, ok := r.Watermark("shows")
	require.True(t, ok)
	require.Equal(t, uint64(100), watermark)

	_, ok = r.Watermark("absent")
	require.False(t, ok)

	require.Equal(t, uint64(100), r.MinWatermark())
}

func TestRuntimeMinWatermarkIsZeroWithoutWatermarks(t *testing.T) {
	require.Zero(t, testRuntime(t).MinWatermark())
}

func TestRuntimeRejectsUndeclaredInternalState(t *testing.T) {
	r := testRuntime(t)

	_, err := r.InternalState("unlisted")
	require.ErrorIs(t, err, ErrUnknownState)

	_, err = OpenRawState(r, "unlisted", keyedInput(t, "ru"))
	require.ErrorIs(t, err, ErrUnknownState)

	require.ErrorIs(t, r.LoadInternalState("unlisted", stateKey(t, 1, "ru"), InternalState{}), ErrUnknownState)
}

func TestRuntimeRejectsMalformedExternalStateName(t *testing.T) {
	r := testRuntime(t)

	for _, name := range []string{"state", "/", "/state/", "//state"} {
		_, err := r.ExternalState(name)
		require.ErrorIsf(t, err, ErrInvalidStateName, "name %q", name)
	}

	_, err := r.ExternalState("/unlisted")
	require.ErrorIs(t, err, ErrUnknownState)
}

func TestRuntimeSeparatesOwnedAndJoinedStates(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")

	_, err := OpenExternalState(r, "/joined", input)
	require.ErrorIs(t, err, ErrUnknownState)

	_, err = OpenJoinedExternalState(r, "/state", input)
	require.ErrorIs(t, err, ErrUnknownState)
}

func TestRawStateAccessorLifecycle(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")

	state, err := OpenRawState(r, "counters", input)
	require.NoError(t, err)

	_, ok := state.Get()
	require.False(t, ok)
	require.Equal(t, []byte("fallback"), state.Or([]byte("fallback")))

	require.NoError(t, state.Set([]byte("written")))
	data, ok := state.Get()
	require.True(t, ok)
	require.Equal(t, []byte("written"), data)
	require.Equal(t, []byte("written"), state.Or([]byte("fallback")))

	require.NoError(t, state.Clear())
	_, ok = state.Get()
	require.False(t, ok)
	require.Equal(t, []byte("fallback"), state.Or([]byte("fallback")))

	require.Equal(t, []string{"counters"}, modifiedStateNames(r.ModifiedInternalStates()))
	_, values := collectStates(r.internal["counters"].Modified())
	require.Equal(t, []InternalState{{Reset: true}}, values)
}

func TestRawStateAccessorRejectsEmptyValue(t *testing.T) {
	state, err := OpenRawState(testRuntime(t), "counters", keyedInput(t, "ru"))
	require.NoError(t, err)
	require.ErrorIs(t, state.Set(nil), ErrEmptyStateValue)
}

func TestRawStateAccessorOwnsStoredBytes(t *testing.T) {
	state, err := OpenRawState(testRuntime(t), "counters", keyedInput(t, "ru"))
	require.NoError(t, err)

	data := []byte("stored")
	require.NoError(t, state.Set(data))
	data[0] = 'X'

	read, ok := state.Get()
	require.True(t, ok)
	require.Equal(t, []byte("stored"), read)

	read[0] = 'Y'
	read, ok = state.Get()
	require.True(t, ok)
	require.Equal(t, []byte("stored"), read)
}

func TestRawStateAccessorReadsRequestState(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")
	require.NoError(t, r.LoadInternalState("counters", input.Key, InternalState{Data: []byte("loaded")}))

	state, err := OpenRawState(r, "counters", input)
	require.NoError(t, err)

	data, ok := state.Get()
	require.True(t, ok)
	require.Equal(t, []byte("loaded"), data)

	require.Empty(t, modifiedStateNames(r.ModifiedInternalStates()))
}

func TestStateAccessorsAreKeyedByTheirInput(t *testing.T) {
	r := testRuntime(t)

	message := keyedInput(t, "ru")
	timer := Timer{Key: stateKey(t, 1, "tr")}

	messageState, err := OpenRawState(r, "counters", message)
	require.NoError(t, err)
	require.NoError(t, messageState.Set([]byte("ru")))

	timerState, err := OpenRawState(r, "counters", timer)
	require.NoError(t, err)

	_, ok := timerState.Get()
	require.False(t, ok)

	require.NoError(t, timerState.Set([]byte("tr")))
	data, ok := messageState.Get()
	require.True(t, ok)
	require.Equal(t, []byte("ru"), data)

	keys, _ := collectStates(r.internal["counters"].Modified())
	require.Equal(t, []string{"ru", "tr"}, regions(t, keys))
}

func TestYSONStateLifecycle(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")

	state, err := OpenYSONState[windowState](r, "windows", input)
	require.NoError(t, err)
	require.True(t, state.Empty())

	value := state.Value()
	value.Count = 7
	value.Label = "open"

	same, err := OpenYSONState[windowState](r, "windows", input)
	require.NoError(t, err)
	require.Same(t, state, same)
	require.Equal(t, windowState{Count: 7, Label: "open"}, *same.Value())

	require.NoError(t, r.flushYSONStates())

	state, err = OpenYSONState[windowState](r, "windows", input)
	require.NoError(t, err)
	require.False(t, state.Empty())
	require.Equal(t, windowState{Count: 7, Label: "open"}, *state.Value())

	state.Clear()
	require.NoError(t, r.flushYSONStates())

	raw, err := OpenRawState(r, "windows", input)
	require.NoError(t, err)
	_, ok := raw.Get()
	require.False(t, ok)
}

func TestYSONStateReadDoesNotWriteAbsentValue(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")

	state, err := OpenYSONState[windowState](r, "windows", input)
	require.NoError(t, err)
	require.Zero(t, *state.Value())
	require.NoError(t, r.flushYSONStates())

	require.Empty(t, modifiedStateNames(r.ModifiedInternalStates()))
	raw, err := OpenRawState(r, "windows", input)
	require.NoError(t, err)
	_, ok := raw.Get()
	require.False(t, ok)
}

func TestYSONStateReadDoesNotRewritePresentValue(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")
	require.NoError(t, r.LoadInternalState(
		"windows",
		input.Key,
		InternalState{Data: []byte(`{count=7;label="open"}`)},
	))

	state, err := OpenYSONState[windowState](r, "windows", input)
	require.NoError(t, err)
	require.Equal(t, windowState{Count: 7, Label: "open"}, *state.Value())
	require.NoError(t, r.flushYSONStates())

	require.Empty(t, modifiedStateNames(r.ModifiedInternalStates()))
}

func TestOpenYSONStateReportsDecodeFailure(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")
	require.NoError(t, r.LoadInternalState("windows", input.Key, InternalState{Data: []byte("}{")}))

	_, err := OpenYSONState[windowState](r, "windows", input)
	require.Error(t, err)
}

func TestYSONStateIsNotFlushedAfterHandlerFailure(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")
	computation := NewRowComputation("failing", RowFunc(func(
		_ context.Context,
		rt Runtime,
		msg ExtendedMessage,
		_ OutputCollector,
	) error {
		state, err := OpenYSONState[windowState](rt, "windows", msg)
		if err != nil {
			return err
		}
		state.Value().Count++
		return xerrors.New("failed")
	}))

	_, err := computation.Process(context.Background(), r, Batch{Messages: []ExtendedMessage{input}})
	require.Error(t, err)

	raw, err := OpenRawState(r, "windows", input)
	require.NoError(t, err)
	_, ok := raw.Get()
	require.False(t, ok)
}

func TestProtoStateAccessorLifecycle(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")

	state, err := OpenProtoState[companion.TWatermark](r, "counters", input)
	require.NoError(t, err)

	_, ok, err := state.Get()
	require.NoError(t, err)
	require.False(t, ok)

	fallback, err := state.Or(&companion.TWatermark{})
	require.NoError(t, err)
	require.Equal(t, "", fallback.GetStreamId())

	require.NoError(t, state.Set(&companion.TWatermark{
		StreamId:  ptr("clicks"),
		Watermark: ptr(uint64(42)),
	}))

	stored, ok, err := state.Get()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "clicks", stored.GetStreamId())
	require.Equal(t, uint64(42), stored.GetWatermark())

	require.NoError(t, state.Clear())
	_, ok, err = state.Get()
	require.NoError(t, err)
	require.False(t, ok)
}

func TestProtoStateAccessorReportsDecodeFailure(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")
	require.NoError(t, r.LoadInternalState("counters", input.Key, InternalState{Data: []byte{0xFF, 0xFF, 0xFF}}))

	state, err := OpenProtoState[companion.TWatermark](r, "counters", input)
	require.NoError(t, err)

	_, _, err = state.Get()
	require.Error(t, err)
}

func TestProtoStateAccessorRejectsEmptyValueAtSet(t *testing.T) {
	r := testRuntime(t)
	state, err := OpenProtoState[companion.TReqCompanionInfo](r, "counters", keyedInput(t, "ru"))
	require.NoError(t, err)

	require.ErrorIs(t, state.Set(&companion.TReqCompanionInfo{}), ErrEmptyStateValue)
	require.Empty(t, modifiedStateNames(r.ModifiedInternalStates()))
}

type externalStateValue struct {
	Count int64  `yson:"count"`
	Label string `yson:"label"`
}

func TestExternalStateAccessorConvertsTypedValue(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")
	require.NoError(t, r.LoadExternalState(
		"/state", externalStateSchema(), input.Key,
		ExternalState{Value: externalStateRow(t, 10, "loaded")}))

	state, err := OpenExternalState(r, "/state", input)
	require.NoError(t, err)

	var value externalStateValue
	exists, err := state.ConvertTo(&value)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, externalStateValue{Count: 10, Label: "loaded"}, value)

	value.Count++
	require.NoError(t, state.ConvertFrom(&value))

	_, values := collectStates(r.external["/state"].Modified())
	require.Len(t, values, 1)
	var updated externalStateValue
	require.NoError(t, values[0].Value.ConvertTo(&updated))
	require.Equal(t, externalStateValue{Count: 11, Label: "loaded"}, updated)
}

func TestExternalStateAccessorRejectsMissingRowAtSet(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")
	require.NoError(t, r.LoadExternalState(
		"/state",
		externalStateSchema(),
		input.Key,
		ExternalState{Value: NewPayload(nil, externalStateSchema())},
	))

	state, err := OpenExternalState(r, "/state", input)
	require.NoError(t, err)
	require.ErrorIs(t, state.Set(Payload{}), ErrEmptyStateValue)
}

func TestExternalStateAccessorLifecycle(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")
	require.NoError(t, r.LoadExternalState(
		"/state", externalStateSchema(), input.Key,
		ExternalState{Value: externalStateRow(t, 10, "loaded")}))

	state, err := OpenExternalState(r, "/state", input)
	require.NoError(t, err)
	require.Equal(t, externalStateSchema().Columns(), state.Schema().Columns())

	row, ok := state.Get()
	require.True(t, ok)
	count, err := row.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(10), count)

	require.Empty(t, modifiedStateNames(r.ModifiedExternalStates()))

	updated, err := state.Builder().Set("count", count+1).Finish()
	require.NoError(t, err)
	require.NoError(t, state.Set(updated))

	require.Equal(t, []string{"/state"}, modifiedStateNames(r.ModifiedExternalStates()))
	_, values := collectStates(r.external["/state"].Modified())
	require.Len(t, values, 1)
	require.False(t, values[0].Reset)
	count, err = values[0].Value.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(11), count)

	label, err := values[0].Value.String("label")
	require.NoError(t, err)
	require.Equal(t, "loaded", label)
}

func TestExternalStateAccessorBuildsRowForUnseenKey(t *testing.T) {
	r := testRuntime(t)
	require.NoError(t, r.LoadExternalState(
		"/state", externalStateSchema(), stateKey(t, 1, "ru"),
		ExternalState{Value: externalStateRow(t, 10, "loaded")}))

	fresh := keyedInput(t, "tr")
	state, err := OpenExternalState(r, "/state", fresh)
	require.NoError(t, err)

	_, ok := state.Get()
	require.False(t, ok)

	row, err := state.Builder().Set("count", int64(1)).Finish()
	require.NoError(t, err)
	require.NoError(t, state.Set(row))

	keys, values := collectStates(r.external["/state"].Modified())
	require.Equal(t, []string{"tr"}, regions(t, keys))
	count, err := values[0].Value.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestExternalStateAccessorClearRequestsDeletion(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")
	require.NoError(t, r.LoadExternalState(
		"/state", externalStateSchema(), input.Key,
		ExternalState{Value: externalStateRow(t, 10, "loaded")}))

	state, err := OpenExternalState(r, "/state", input)
	require.NoError(t, err)
	require.NoError(t, state.Clear())

	_, ok := state.Get()
	require.False(t, ok)
	require.Equal(t, externalStateRow(t, 0, "gone"), state.Or(externalStateRow(t, 0, "gone")))

	_, values := collectStates(r.external["/state"].Modified())
	require.Len(t, values, 1)
	require.True(t, values[0].Reset)
}

func TestRuntimeExternalStateNotReadByTheRequest(t *testing.T) {
	r := testRuntime(t)

	_, err := OpenExternalState(r, "/aux/state", keyedInput(t, "ru"))
	require.ErrorIs(t, err, ErrStateNotRead)
	require.NotErrorIs(t, err, ErrNoStateSchema)
}

func TestRuntimeExternalStateNeedsTheRequestSchema(t *testing.T) {
	r := testRuntime(t)

	err := r.LoadExternalState(
		"/aux/state", Schema{}, stateKey(t, 1, "ru"),
		ExternalState{Value: externalStateRow(t, 1, "x")})
	require.ErrorIs(t, err, ErrNoStateSchema)
	require.NotErrorIs(t, err, ErrStateNotRead)
}

func TestJoinedExternalStateAccessorReads(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")
	require.NoError(t, r.LoadJoinedExternalState(
		"/joined", externalStateSchema(), input.Key,
		ExternalState{Value: externalStateRow(t, 5, "profile")}))

	state, err := OpenJoinedExternalState(r, "/joined", input)
	require.NoError(t, err)
	require.Equal(t, externalStateSchema().Columns(), state.Schema().Columns())

	row, ok := state.Get()
	require.True(t, ok)
	label, err := row.String("label")
	require.NoError(t, err)
	require.Equal(t, "profile", label)

	var value externalStateValue
	exists, err := state.ConvertTo(&value)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, externalStateValue{Count: 5, Label: "profile"}, value)

	missing, err := OpenJoinedExternalState(r, "/joined", keyedInput(t, "tr"))
	require.NoError(t, err)
	_, ok = missing.Get()
	require.False(t, ok)
	require.Equal(t, externalStateRow(t, 0, "default"), missing.Or(externalStateRow(t, 0, "default")))
}

func TestJoinedExternalStateIsNeverEchoedBack(t *testing.T) {
	r := testRuntime(t)
	key := stateKey(t, 1, "ru")
	require.NoError(t, r.LoadJoinedExternalState(
		"/joined", externalStateSchema(), key,
		ExternalState{Value: externalStateRow(t, 5, "profile")}))

	holder, err := r.JoinedExternalState("/joined")
	require.NoError(t, err)
	require.NoError(t, holder.Set(key, ExternalState{Value: externalStateRow(t, 6, "overwritten")}))

	require.Empty(t, modifiedStateNames(r.ModifiedExternalStates()))
}

func TestModifiedStatesReportOnlyWrittenHolders(t *testing.T) {
	r := testRuntime(t)
	input := keyedInput(t, "ru")

	require.NoError(t, r.LoadInternalState("counters", input.Key, InternalState{Data: []byte("loaded")}))
	require.NoError(t, r.LoadExternalState(
		"/state", externalStateSchema(), input.Key,
		ExternalState{Value: externalStateRow(t, 10, "loaded")}))

	windows, err := OpenRawState(r, "windows", input)
	require.NoError(t, err)
	require.NoError(t, windows.Set([]byte("written")))

	counters, err := OpenRawState(r, "counters", input)
	require.NoError(t, err)
	_, ok := counters.Get()
	require.True(t, ok)

	require.Equal(t, []string{"windows"}, modifiedStateNames(r.ModifiedInternalStates()))
	require.Empty(t, modifiedStateNames(r.ModifiedExternalStates()))
}

func ptr[T any](value T) *T {
	return &value
}
