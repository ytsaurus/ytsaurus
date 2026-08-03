package flowtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/schema"
)

type fatalTB struct {
	testing.TB

	failure string
}

type fatalPanic struct{}

func (tb *fatalTB) Helper() {}

func (tb *fatalTB) Fatalf(format string, args ...any) {
	tb.failure = fmt.Sprintf(format, args...)
	panic(fatalPanic{})
}

func failure(t *testing.T, fn func(tb testing.TB)) (reported string) {
	t.Helper()

	tb := &fatalTB{}
	defer func() {
		recovered := recover()
		if _, ok := recovered.(fatalPanic); !ok {
			if recovered != nil {
				panic(recovered)
			}
			t.Fatalf("expected the harness to fail the test, it did not")
		}
		reported = tb.failure
	}()

	fn(tb)
	return ""
}

var (
	wordSchema  = Schema("word:string")
	countSchema = Schema("word:string", "count:int64")
)

type counter struct{}

type counterState struct {
	Word  string `yson:"word"`
	Count int64  `yson:"count"`
}

func (counter) OnMessage(_ context.Context, rt flow.Runtime, msg flow.ExtendedMessage, out flow.OutputCollector) error {
	word, err := msg.Payload.String("word")
	if err != nil {
		return err
	}

	state, err := flow.OpenYSONState[counterState](rt, "words", msg)
	if err != nil {
		return err
	}
	fresh := state.Empty()
	stored := state.Value()
	if fresh {
		stored.Word = word
	}
	stored.Count++

	b, err := rt.MessageBuilder("counts")
	if err != nil {
		return err
	}
	count, err := b.Set("word", word).Set("count", stored.Count).Finish()
	if err != nil {
		return err
	}
	out.AddMessage(count)

	return nil
}

func counterHarness(tb testing.TB) *Harness {
	return New(tb, flow.NewRowComputation("counter", counter{}), Options{
		Streams: map[string]flow.Schema{
			"words":  wordSchema,
			"counts": countSchema,
		},
		KeySchema:      wordSchema,
		InternalStates: []string{"words"},
	})
}

func TestHarnessRunsARowComputationPerMessage(t *testing.T) {
	h := counterHarness(t)
	key := h.Key(Row{"word": "hello"})

	r := h.Process(
		h.KeyedMessage("words", key, Row{"word": "hello"}),
		h.KeyedMessage("words", key, Row{"word": "hello"}),
	)

	require.Len(t, r.Groups(), 2)
	require.Equal(t, []Row{
		{"word": "hello", "count": int64(1)},
		{"word": "hello", "count": int64(2)},
	}, r.Rows())
	require.Equal(t, []bool{true, true}, r.Distribute())
}

func TestHarnessAttributesOutputToItsInput(t *testing.T) {
	h := counterHarness(t)
	key := h.Key(Row{"word": "hello"})
	msg := h.KeyedMessage("words", key, Row{"word": "hello"})

	r := h.Process(msg)

	require.Len(t, r.Groups(), 1)
	require.Equal(t, []string{msg.ID}, r.Groups()[0].ParentIDs)
}

func TestHarnessKeepsStateOfDifferentKeysApart(t *testing.T) {
	h := counterHarness(t)
	foo := h.Key(Row{"word": "foo"})
	bar := h.Key(Row{"word": "bar"})

	r := h.Process(
		h.KeyedMessage("words", foo, Row{"word": "foo"}),
		h.KeyedMessage("words", bar, Row{"word": "bar"}),
		h.KeyedMessage("words", foo, Row{"word": "foo"}),
	)

	var state counterState
	require.True(t, r.InternalStateYSON("words", foo, &state))
	require.Equal(t, counterState{Word: "foo", Count: 2}, state)

	require.True(t, r.InternalStateYSON("words", bar, &state))
	require.Equal(t, counterState{Word: "bar", Count: 1}, state)

	require.Equal(t, 2, r.InternalStateLen("words"))
	require.True(t, r.InternalStateWritten("words"))
}

func TestHarnessReadsPrepopulatedInternalState(t *testing.T) {
	h := counterHarness(t)
	key := h.Key(Row{"word": "hello"})
	h.PutInternalStateYSON("words", key, counterState{Word: "hello", Count: 41})

	r := h.Process(h.KeyedMessage("words", key, Row{"word": "hello"}))

	var state counterState
	require.True(t, r.InternalStateYSON("words", key, &state))
	require.Equal(t, int64(42), state.Count)
}

func TestHarnessCarriesStateFromOneRunToTheNext(t *testing.T) {
	h := counterHarness(t)
	key := h.Key(Row{"word": "hello"})

	var last *Response
	for range 3 {
		last = h.Process(h.KeyedMessage("words", key, Row{"word": "hello"}))
	}

	var state counterState
	require.True(t, last.InternalStateYSON("words", key, &state))
	require.Equal(t, int64(3), state.Count)
}

func TestHarnessKeysAreCompared(t *testing.T) {
	h := counterHarness(t)
	h.PutInternalStateYSON("words", h.Key(Row{"word": "hello"}), counterState{Count: 7})

	r := h.Process(h.KeyedMessage("words", h.Key(Row{"word": "hello"}), Row{"word": "hello"}))

	var state counterState
	require.True(t, r.InternalStateYSON("words", h.Key(Row{"word": "hello"}), &state))
	require.Equal(t, int64(8), state.Count)
}

type eraser struct {
	counter
}

func (eraser) OnTimer(_ context.Context, rt flow.Runtime, timer flow.Timer, _ flow.OutputCollector) error {
	state, err := flow.OpenRawState(rt, "words", timer)
	if err != nil {
		return err
	}
	return state.Clear()
}

func TestHarnessReportsClearedInternalState(t *testing.T) {
	h := New(t, flow.NewRowComputation("eraser", eraser{}), Options{
		Streams:        map[string]flow.Schema{"words": wordSchema, "counts": countSchema},
		KeySchema:      wordSchema,
		InternalStates: []string{"words"},
	})
	key := h.Key(Row{"word": "hello"})
	h.PutInternalStateYSON("words", key, counterState{Count: 1})

	r := h.Process(h.Timer(key, 100))

	_, ok := r.InternalStateRaw("words", key)
	require.False(t, ok)
	require.True(t, r.InternalStateReset("words", key))

	next := h.Process(h.KeyedMessage("words", key, Row{"word": "hello"}))
	var state counterState
	require.True(t, next.InternalStateYSON("words", key, &state))
	require.Equal(t, int64(1), state.Count)
}

type timerRecorder struct {
	counter

	fired []flow.Timer
}

func (r *timerRecorder) OnTimer(_ context.Context, _ flow.Runtime, timer flow.Timer, _ flow.OutputCollector) error {
	r.fired = append(r.fired, timer)
	return nil
}

func TestHarnessDeliversATimerOfANamedStream(t *testing.T) {
	recorder := &timerRecorder{}
	h := New(t, flow.NewRowComputation("recorder", recorder), Options{
		Streams:   map[string]flow.Schema{"words": wordSchema},
		KeySchema: wordSchema,
	})
	key := h.Key(Row{"word": "hello"})

	unnamed := h.Timer(key, 100)
	named := h.Timer(key, 200)
	named.StreamID = "hourly"

	h.Process(unnamed, named)

	require.Len(t, recorder.fired, 2)
	require.Empty(t, recorder.fired[0].StreamID)
	require.Equal(t, uint64(100), recorder.fired[0].TriggerTimestamp)
	require.Equal(t, "hourly", recorder.fired[1].StreamID)
	require.Equal(t, key, recorder.fired[1].Key)
}

type visitTimer struct {
	counter
}

func (visitTimer) OnVisit(_ context.Context, _ flow.Runtime, _ flow.Visit, out flow.OutputCollector) error {
	out.AddTimer(flow.TimerRequest{TriggerTimestamp: 500})
	return nil
}

func TestHarnessDeliversVisitsAndCollectsTimers(t *testing.T) {
	h := New(t, flow.NewRowComputation("visitor", visitTimer{}), Options{
		Streams:        map[string]flow.Schema{"words": wordSchema, "counts": countSchema},
		KeySchema:      wordSchema,
		InternalStates: []string{"words"},
	})

	r := h.Process(h.Visit(h.Key(Row{"word": "hello"})))

	require.Empty(t, r.Messages())
	require.Equal(t, []flow.TimerRequest{{TriggerTimestamp: 500}}, r.Timers())
}

type batchSummer struct{}

func (batchSummer) OnMessages(_ context.Context, rt flow.Runtime, msgs []flow.ExtendedMessage, out flow.OutputCollector) error {
	b, err := rt.MessageBuilder("counts")
	if err != nil {
		return err
	}
	msg, err := b.Set("word", "batch").Set("count", int64(len(msgs))).Finish()
	if err != nil {
		return err
	}
	out.AddMessage(msg)
	return nil
}

func TestHarnessRunsABatchComputationOnce(t *testing.T) {
	h := New(t, flow.NewBatchComputation("summer", batchSummer{}), Options{
		Streams:   map[string]flow.Schema{"words": wordSchema, "counts": countSchema},
		KeySchema: wordSchema,
	})
	key := h.Key(Row{"word": "hello"})

	r := h.Process(
		h.KeyedMessage("words", key, Row{"word": "a"}),
		h.KeyedMessage("words", key, Row{"word": "b"}),
	)

	require.Len(t, r.Groups(), 1)
	require.Equal(t, []Row{{"word": "batch", "count": int64(2)}}, r.Rows())
}

type filter struct {
	keep string
}

func (f filter) OnMessage(_ context.Context, rt flow.Runtime, msg flow.ExtendedMessage, out flow.OutputCollector) error {
	word, err := msg.Payload.String("word")
	if err != nil {
		return err
	}
	b, err := rt.MessageBuilder("counts")
	if err != nil {
		return err
	}
	passed, err := b.Set("word", word).Finish()
	if err != nil {
		return err
	}
	if word == f.keep {
		out.AddMessage(passed)
	} else {
		out.AddUndistributedMessage(passed)
	}
	return nil
}

func TestHarnessReportsDistributeFlags(t *testing.T) {
	h := New(t, flow.NewRowSourceComputation("filter", filter{keep: "yes"}), Options{
		Streams: map[string]flow.Schema{"words": wordSchema, "counts": countSchema},
	})

	r := h.Process(
		h.Message("words", Row{"word": "yes"}),
		h.Message("words", Row{"word": "no"}),
	)

	require.Equal(t, []bool{true, false}, r.Distribute())
}

func TestHarnessReadsParametersAndWatermarks(t *testing.T) {
	var (
		static, dynamic int
		watermark       uint64
		minWatermark    uint64
	)
	computation := flow.NewRowComputation("params", flow.RowFunc(
		func(_ context.Context, rt flow.Runtime, _ flow.ExtendedMessage, _ flow.OutputCollector) error {
			if err := rt.Parameters().Get("window", &static); err != nil {
				return err
			}
			if err := rt.DynamicParameters().Get("window", &dynamic); err != nil {
				return err
			}
			watermark, _ = rt.Watermark("words")
			minWatermark = rt.MinWatermark()
			return nil
		}))

	h := New(t, computation, Options{
		Streams:           map[string]flow.Schema{"words": wordSchema},
		Parameters:        map[string]any{"window": 100},
		DynamicParameters: map[string]any{"window": 200},
	})
	h.SetWatermark("words", 42)

	h.Process(h.Message("words", Row{"word": "hello"}))

	require.Equal(t, 100, static)
	require.Equal(t, 200, dynamic)
	require.Equal(t, uint64(42), watermark)
	require.Equal(t, uint64(42), minWatermark)
}

type failing struct{}

func (failing) OnMessage(context.Context, flow.Runtime, flow.ExtendedMessage, flow.OutputCollector) error {
	return fmt.Errorf("payload is not what it should be")
}

func TestHarnessReturnsTheErrorAComputationFailedWith(t *testing.T) {
	h := New(t, flow.NewRowComputation("failing", failing{}), Options{
		Streams: map[string]flow.Schema{"words": wordSchema},
	})

	err := h.ProcessError(h.Message("words", Row{"word": "hello"}))
	require.ErrorContains(t, err, "payload is not what it should be")
}

func TestHarnessFailsTheTestOnAFailedComputation(t *testing.T) {
	reported := failure(t, func(tb testing.TB) {
		h := New(tb, flow.NewRowComputation("failing", failing{}), Options{
			Streams: map[string]flow.Schema{"words": wordSchema},
		})
		h.Process(h.Message("words", Row{"word": "hello"}))
	})
	require.Contains(t, reported, "payload is not what it should be")
}

func TestHarnessFailsWhenAComputationDoesNot(t *testing.T) {
	reported := failure(t, func(tb testing.TB) {
		h := counterHarness(tb)
		key := h.Key(Row{"word": "hello"})
		_ = h.ProcessError(h.KeyedMessage("words", key, Row{"word": "hello"}))
	})
	require.Contains(t, reported, "without an error")
}

type undeclaredOutput struct{}

func (undeclaredOutput) OnMessage(_ context.Context, _ flow.Runtime, _ flow.ExtendedMessage, out flow.OutputCollector) error {
	out.AddMessage(flow.Message{Meta: flow.Meta{StreamID: "elsewhere"}})
	return nil
}

func TestHarnessRefusesOutputOnAnUndeclaredStream(t *testing.T) {
	h := New(t, flow.NewRowComputation("stray", undeclaredOutput{}), Options{
		Streams: map[string]flow.Schema{"words": wordSchema},
	})

	err := h.ProcessError(h.Message("words", Row{"word": "hello"}))
	require.ErrorIs(t, err, flow.ErrUnknownStream)
}

type blanker struct{}

func (blanker) OnMessage(_ context.Context, rt flow.Runtime, msg flow.ExtendedMessage, _ flow.OutputCollector) error {
	state, err := flow.OpenRawState(rt, "words", msg)
	if err != nil {
		return err
	}
	return state.Set(nil)
}

func TestHarnessRefusesAStateWrittenAsEmptyBytes(t *testing.T) {
	h := New(t, flow.NewRowComputation("blanker", blanker{}), Options{
		Streams:        map[string]flow.Schema{"words": wordSchema},
		KeySchema:      wordSchema,
		InternalStates: []string{"words"},
	})

	err := h.ProcessError(h.KeyedMessage("words", h.Key(Row{"word": "hello"}), Row{"word": "hello"}))
	require.ErrorIs(t, err, flow.ErrEmptyStateValue)
}

func TestHarnessFailsOnAnUnknownStream(t *testing.T) {
	reported := failure(t, func(tb testing.TB) {
		counterHarness(tb).Message("nowhere", Row{"word": "hello"})
	})
	require.Contains(t, reported, `unknown stream "nowhere"`)
}

func TestHarnessFailsOnAnUnknownColumn(t *testing.T) {
	reported := failure(t, func(tb testing.TB) {
		counterHarness(tb).Message("words", Row{"nonesuch": "hello"})
	})
	require.Contains(t, reported, "column not found")
}

func TestHarnessFailsOnAnUndeclaredState(t *testing.T) {
	reported := failure(t, func(tb testing.TB) {
		h := counterHarness(tb)
		h.PutInternalState("nonesuch", h.Key(Row{"word": "hello"}), []byte("x"))
	})
	require.Contains(t, reported, "undeclared state")
}

func TestHarnessFailsOnAnUnknownWatermarkStream(t *testing.T) {
	reported := failure(t, func(tb testing.TB) {
		counterHarness(tb).SetWatermark("nowhere", 1)
	})
	require.Contains(t, reported, `unknown stream "nowhere"`)
}

func TestHarnessFailsOnAnInputThatIsNeitherMessageTimerNorVisit(t *testing.T) {
	reported := failure(t, func(tb testing.TB) {
		counterHarness(tb).Process(strayInput{})
	})
	require.Contains(t, reported, "want a message, a timer or a visit")
}

type strayInput struct{}

func (strayInput) PartitionKey() flow.Payload { return flow.Payload{} }

func TestSchemaBuildsColumnsInOrder(t *testing.T) {
	s := Schema("word:string", "count:int64", "ratio:double")

	require.Equal(t, 3, s.Len())
	name, ok := s.ColumnName(1)
	require.True(t, ok)
	require.Equal(t, "count", name)

	id, ok := s.FindColumn("ratio")
	require.True(t, ok)
	require.Equal(t, 2, id)
}

func TestSchemaResolvesTypeNamesThroughTheSchemaPackage(t *testing.T) {
	columns := Schema("flag:bool", "blob:yson", "id:uuid").Table().Columns

	require.Equal(t, schema.TypeBoolean, columns[0].Type)
	require.Equal(t, schema.TypeAny, columns[1].Type)
	require.Equal(t, schema.Type("uuid"), columns[2].Type)
}

func TestSchemaPanicsOnAMalformedColumn(t *testing.T) {
	require.PanicsWithValue(t, `flowtest: column "word" is not spelled name:type`, func() {
		Schema("word")
	})
}

func TestSchemaPanicsOnAnUnknownType(t *testing.T) {
	require.PanicsWithValue(t, `flowtest: column "word:varchar": unknown type "varchar"`, func() {
		Schema("word:varchar")
	})
}

type wordRow struct {
	Word  string `yson:"word"`
	Count int64  `yson:"count"`
}

func TestSchemaOfTakesColumnsFromTheStructTags(t *testing.T) {
	columns := SchemaOf(wordRow{}).Table().Columns

	require.Len(t, columns, 2)
	require.Equal(t, "word", columns[0].Name)
	require.Equal(t, schema.TypeString, columns[0].Type)
	require.Equal(t, "count", columns[1].Name)
	require.Equal(t, schema.TypeInt64, columns[1].Type)
}

func TestSchemaOfRoundTripsTheStructItWasBuiltFrom(t *testing.T) {
	written := wordRow{Word: "hello", Count: 2}

	p, err := flow.NewPayloadBuilder(SchemaOf(wordRow{})).SetStruct(written).Finish()
	require.NoError(t, err)

	var read wordRow
	require.NoError(t, p.ConvertTo(&read))
	require.Equal(t, written, read)
}

func TestSchemaOfPanicsOnAValueThatIsNotAStruct(t *testing.T) {
	require.Panics(t, func() { SchemaOf(42) })
}

func TestToRowDecodesEveryValueKind(t *testing.T) {
	s := Schema("text:string", "n:int64", "u:uint64", "ratio:double", "flag:boolean", "blob:any")
	p, err := flow.NewPayloadBuilder(s).
		Set("text", "hello").
		Set("n", int64(-1)).
		Set("u", uint64(2)).
		Set("ratio", 0.5).
		Set("flag", true).
		Set("blob", map[string]any{"a": int64(1)}).
		Finish()
	require.NoError(t, err)

	row := ToRow(p)

	require.Equal(t, "hello", row["text"])
	require.Equal(t, int64(-1), row["n"])
	require.Equal(t, uint64(2), row["u"])
	require.InDelta(t, 0.5, row["ratio"], 1e-9)
	require.Equal(t, true, row["flag"])
	require.IsType(t, []byte(nil), row["blob"])
}

func TestToRowSkipsNullCells(t *testing.T) {
	p, err := flow.NewPayloadBuilder(countSchema).Set("word", "hello").Finish()
	require.NoError(t, err)

	require.Equal(t, Row{"word": "hello"}, ToRow(p))
}
