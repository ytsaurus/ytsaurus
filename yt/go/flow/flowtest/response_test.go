package flowtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/proto/flow/companion"
)

var (
	eventSchema = Schema("user:string", "amount:int64")
	stateSchema = Schema("count:int64")
	profSchema  = Schema("plan:string")
)

type accumulator struct{}

func (accumulator) OnMessage(_ context.Context, rt flow.Runtime, msg flow.ExtendedMessage, _ flow.OutputCollector) error {
	state, err := flow.OpenExternalState(rt, "/counts", msg)
	if err != nil {
		return err
	}

	count, _ := state.Get()
	stored, _ := count.Int64("count")

	row, err := state.Builder().Set("count", stored+1).Finish()
	if err != nil {
		return err
	}
	return state.Set(row)
}

func accumulatorHarness(tb testing.TB) *Harness {
	return New(tb, flow.NewRowComputation("accumulator", accumulator{}), Options{
		Streams:        map[string]flow.Schema{"events": eventSchema},
		KeySchema:      Schema("user:string"),
		ExternalStates: map[string]flow.Schema{"/counts": stateSchema},
	})
}

func TestResponseReportsExternalStateWrittenForAFreshKey(t *testing.T) {
	h := accumulatorHarness(t)
	key := h.Key(Row{"user": "bob"})

	r := h.Process(h.KeyedMessage("events", key, Row{"user": "bob", "amount": int64(1)}))

	require.Equal(t, Row{"count": int64(1)}, r.ExternalStateRow("/counts", key))
	require.True(t, r.ExternalStateWritten("/counts"))
}

func TestResponseReportsExternalStateOfEachKey(t *testing.T) {
	h := accumulatorHarness(t)
	bob := h.Key(Row{"user": "bob"})
	eve := h.Key(Row{"user": "eve"})
	h.PutExternalState("/counts", eve, Row{"count": int64(10)})

	r := h.Process(
		h.KeyedMessage("events", bob, Row{"user": "bob"}),
		h.KeyedMessage("events", eve, Row{"user": "eve"}),
		h.KeyedMessage("events", bob, Row{"user": "bob"}),
	)

	require.Equal(t, Row{"count": int64(2)}, r.ExternalStateRow("/counts", bob))
	require.Equal(t, Row{"count": int64(11)}, r.ExternalStateRow("/counts", eve))
	require.Equal(t, 2, r.ExternalStateLen("/counts"))
}

func TestResponseExternalStateSurvivesToTheNextRun(t *testing.T) {
	h := accumulatorHarness(t)
	key := h.Key(Row{"user": "bob"})

	h.Process(h.KeyedMessage("events", key, Row{"user": "bob"}))
	r := h.Process(h.KeyedMessage("events", key, Row{"user": "bob"}))

	require.Equal(t, Row{"count": int64(2)}, r.ExternalStateRow("/counts", key))
}

type resetter struct{}

func (resetter) OnMessage(_ context.Context, rt flow.Runtime, msg flow.ExtendedMessage, _ flow.OutputCollector) error {
	state, err := flow.OpenExternalState(rt, "/counts", msg)
	if err != nil {
		return err
	}

	stored, _ := state.Get()
	if _, err := stored.Int64("count"); err == nil {
		return state.Clear()
	}

	row, err := state.Builder().Set("count", int64(1)).Finish()
	if err != nil {
		return err
	}
	return state.Set(row)
}

func TestResponseReportsDeletedExternalState(t *testing.T) {
	h := New(t, flow.NewRowComputation("resetter", resetter{}), Options{
		Streams:        map[string]flow.Schema{"events": eventSchema},
		KeySchema:      Schema("user:string"),
		ExternalStates: map[string]flow.Schema{"/counts": stateSchema},
	})
	key := h.Key(Row{"user": "bob"})
	h.PutExternalState("/counts", key, Row{"count": int64(3)})

	r := h.Process(h.KeyedMessage("events", key, Row{"user": "bob"}))

	require.Nil(t, r.ExternalStateRow("/counts", key))
	require.True(t, r.ExternalStateReset("/counts", key))

	next := h.Process(h.KeyedMessage("events", key, Row{"user": "bob"}))
	require.Equal(t, Row{"count": int64(1)}, next.ExternalStateRow("/counts", key))
}

func TestResponseReportsAnExternalStateNoRunRead(t *testing.T) {
	h := accumulatorHarness(t)

	r := h.Process()

	require.Nil(t, r.ExternalStateRow("/counts", h.Key(Row{"user": "bob"})))
	require.Zero(t, r.ExternalStateLen("/counts"))
	require.False(t, r.ExternalStateWritten("/counts"))
}

type enricher struct{}

func (enricher) OnMessage(_ context.Context, rt flow.Runtime, msg flow.ExtendedMessage, out flow.OutputCollector) error {
	joined, err := flow.OpenJoinedExternalState(rt, "/profiles", msg)
	if err != nil {
		return err
	}
	profile, ok := joined.Get()
	if !ok {
		return nil
	}
	plan, err := profile.String("plan")
	if err != nil {
		return err
	}

	b, err := rt.MessageBuilder("enriched")
	if err != nil {
		return err
	}
	enriched, err := b.Set("plan", plan).Finish()
	if err != nil {
		return err
	}
	out.AddMessage(enriched)
	return nil
}

func enricherHarness(tb testing.TB) *Harness {
	return New(tb, flow.NewRowComputation("enricher", enricher{}), Options{
		Streams: map[string]flow.Schema{
			"events":   eventSchema,
			"enriched": profSchema,
		},
		KeySchema:            Schema("user:string"),
		JoinedExternalStates: map[string]flow.Schema{"/profiles": profSchema},
	})
}

func TestResponseReportsJoinedExternalState(t *testing.T) {
	h := enricherHarness(t)
	key := h.Key(Row{"user": "bob"})
	h.PutJoinedExternalState("/profiles", key, Row{"plan": "gold"})

	r := h.Process(h.KeyedMessage("events", key, Row{"user": "bob"}))

	require.Equal(t, []Row{{"plan": "gold"}}, r.Rows())
	require.Equal(t, Row{"plan": "gold"}, r.JoinedExternalStateRow("/profiles", key))
}

func TestResponseReportsNoJoinedExternalStateForAnUnknownKey(t *testing.T) {
	h := enricherHarness(t)
	known := h.Key(Row{"user": "bob"})
	unknown := h.Key(Row{"user": "eve"})
	h.PutJoinedExternalState("/profiles", known, Row{"plan": "gold"})

	r := h.Process(
		h.KeyedMessage("events", known, Row{"user": "bob"}),
		h.KeyedMessage("events", unknown, Row{"user": "eve"}),
	)

	require.Equal(t, []Row{{"plan": "gold"}}, r.Rows())
	require.Nil(t, r.JoinedExternalStateRow("/profiles", unknown))
}

func TestHarnessOmitsJoinedStateWhenNoBatchKeyHasARow(t *testing.T) {
	h := enricherHarness(t)
	key := h.Key(Row{"user": "eve"})
	h.PutJoinedExternalState("/profiles", h.Key(Row{"user": "bob"}), Row{"plan": "gold"})

	err := h.ProcessError(h.KeyedMessage("events", key, Row{"user": "eve"}))
	require.ErrorIs(t, err, flow.ErrStateNotRead)
}

func TestResponseSplitsMessagesByStream(t *testing.T) {
	computation := flow.NewRowComputation("fanout", flow.RowFunc(
		func(_ context.Context, rt flow.Runtime, msg flow.ExtendedMessage, out flow.OutputCollector) error {
			for _, streamID := range []string{"left", "right"} {
				b, err := rt.MessageBuilder(streamID)
				if err != nil {
					return err
				}
				msg, err := b.Set("word", streamID).Finish()
				if err != nil {
					return err
				}
				out.AddMessage(msg)
			}
			return nil
		}))

	h := New(t, computation, Options{
		Streams: map[string]flow.Schema{
			"words": wordSchema,
			"left":  wordSchema,
			"right": wordSchema,
		},
	})

	r := h.Process(h.Message("words", Row{"word": "hello"}))

	require.Len(t, r.Messages(), 2)
	require.Len(t, r.MessagesOn("left"), 1)
	require.Equal(t, "right", ToRow(r.MessagesOn("right")[0].Payload)["word"])
	require.Empty(t, r.MessagesOn("words"))
}

func TestResponseFailsOnAnUndeclaredStateName(t *testing.T) {
	reported := failure(t, func(tb testing.TB) {
		h := accumulatorHarness(tb)
		key := h.Key(Row{"user": "bob"})
		h.Process(h.KeyedMessage("events", key, Row{"user": "bob"})).ExternalState("/nonesuch", key)
	})
	require.Contains(t, reported, "undeclared state")
}

type protoCounter struct{}

func (protoCounter) OnMessage(_ context.Context, rt flow.Runtime, msg flow.ExtendedMessage, _ flow.OutputCollector) error {
	state, err := flow.OpenProtoState[companion.TNewTimer](rt, "words", msg)
	if err != nil {
		return err
	}
	stored, err := state.Or(&companion.TNewTimer{})
	if err != nil {
		return err
	}
	stored.TriggerTimestamp = proto.Uint64(stored.GetTriggerTimestamp() + 1)
	return state.Set(stored)
}

func TestResponseReadsInternalStateAsProto(t *testing.T) {
	h := New(t, flow.NewRowComputation("proto-counter", protoCounter{}), Options{
		Streams:        map[string]flow.Schema{"words": wordSchema},
		KeySchema:      wordSchema,
		InternalStates: []string{"words"},
	})
	key := h.Key(Row{"word": "hello"})
	h.PutInternalStateProto("words", key, &companion.TNewTimer{TriggerTimestamp: proto.Uint64(42)})

	r := h.Process(h.KeyedMessage("words", key, Row{"word": "hello"}))

	var state companion.TNewTimer
	require.True(t, r.InternalStateProto("words", key, &state))
	require.Equal(t, uint64(43), state.GetTriggerTimestamp())
}

func TestResponseReadsInternalStateAsRawBytes(t *testing.T) {
	computation := flow.NewRowComputation("reader", flow.RowFunc(
		func(_ context.Context, rt flow.Runtime, msg flow.ExtendedMessage, _ flow.OutputCollector) error {
			state, err := flow.OpenRawState(rt, "words", msg)
			if err != nil {
				return err
			}
			data, _ := state.Get()
			return state.Set(append(data, '!'))
		}))

	raw := New(t, computation, Options{
		Streams:        map[string]flow.Schema{"words": wordSchema},
		KeySchema:      wordSchema,
		InternalStates: []string{"words"},
	})
	key := raw.Key(Row{"word": "hello"})
	raw.PutInternalState("words", key, []byte("opaque"))

	r := raw.Process(raw.KeyedMessage("words", key, Row{"word": "hello"}))

	data, ok := r.InternalStateRaw("words", key)
	require.True(t, ok)
	require.Equal(t, []byte("opaque!"), data)
}
