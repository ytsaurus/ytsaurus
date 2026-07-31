package flow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/proto/flow/common"
	"go.ytsaurus.tech/yt/go/proto/flow/companion"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/wire"
	"go.ytsaurus.tech/yt/go/yson"
)

const (
	clicksSpecID  int64 = 1
	unknownSpecID int64 = 42
)

var protoJobID = guid.FromHalves(0x1122334455667788, 0x99aabbccddeeff00)

func batchKeySchema() Schema {
	return NewSchema(schema.Schema{Columns: []schema.Column{
		{Name: "hash", Type: schema.TypeUint64},
		{Name: "user_id", Type: schema.TypeString},
	}})
}

func batchKey(t *testing.T, hash uint64, userID string) Payload {
	t.Helper()
	key, err := NewPayloadBuilder(batchKeySchema()).Set("hash", hash).Set("user_id", userID).Finish()
	require.NoError(t, err)
	return key
}

func encodeRow(t *testing.T, p Payload) []byte {
	t.Helper()
	encoded, err := wire.MarshalRowProto(p.Row())
	require.NoError(t, err)
	return encoded
}

func encodeSchema(t *testing.T, s Schema) []byte {
	t.Helper()
	encoded, err := yson.Marshal(s.Table())
	require.NoError(t, err)
	return encoded
}

func TestSchemaFromProtoRejectsUnsupportedColumnType(t *testing.T) {
	encoded, err := yson.Marshal(schema.Schema{Columns: []schema.Column{{
		Name: "created_at",
		Type: schema.Type("timestamp64"),
	}}})
	require.NoError(t, err)

	_, err = schemaFromProto(encoded)
	require.ErrorIs(t, err, ErrUnsupportedSchemaType)
}

func protoStream(t *testing.T, streamID string, specID int64, s Schema) *companion.TStream {
	t.Helper()
	return &companion.TStream{
		StreamId:     proto.String(streamID),
		StreamSpecId: proto.Int64(specID),
		Schema:       encodeSchema(t, s),
	}
}

func protoJobInfo(t *testing.T) *companion.TJobInfo {
	t.Helper()
	return &companion.TJobInfo{
		Spec:        []byte(staticSpecYSON),
		DynamicSpec: []byte(dynamicSpecYSON),
		Streams:     []*companion.TStream{protoStream(t, "clicks", clicksSpecID, streamSchema("url"))},
	}
}

func protoJob(t *testing.T) *Job {
	t.Helper()
	job, err := jobFromProto(protoJobID, "counter", protoJobInfo(t))
	require.NoError(t, err)
	return job
}

func clickMessage(t *testing.T, id string, url string, key Payload) *companion.TReqProcessBatch_TExtendedMessage {
	t.Helper()
	payload, err := NewPayloadBuilder(streamSchema("url")).Set("url", url).Finish()
	require.NoError(t, err)

	return &companion.TReqProcessBatch_TExtendedMessage{
		Message: &common.TMessage{
			MessageId:       proto.String(id),
			SystemTimestamp: proto.Uint64(1700),
			EventTimestamp:  proto.Uint64(1600),
			Payload:         encodeRow(t, payload),
			StreamSpecId:    proto.Int64(clicksSpecID),
		},
		Key: encodeRow(t, key),
	}
}

func protoStateItem(t *testing.T, key Payload, state []byte, reset bool) *companion.TStateItem {
	t.Helper()
	return &companion.TStateItem{
		Key:    encodeRow(t, key),
		Reset_: proto.Bool(reset),
		State:  state,
	}
}

func processBatch(t *testing.T, req *companion.TReqProcessBatch) (*RequestRuntime, Batch) {
	t.Helper()
	runtime, batch, err := processBatchFromProto(req, protoJob(t))
	require.NoError(t, err)
	return runtime, batch
}

func TestStreamSpecsFromProtoResolvesStreams(t *testing.T) {
	streams, err := streamSpecsFromProto([]*companion.TStream{
		protoStream(t, "clicks", clicksSpecID, streamSchema("url")),
		protoStream(t, "shows", 7, streamSchema("banner")),
	})
	require.NoError(t, err)

	require.Equal(t, 2, streams.Len())

	streamID, ok := streams.StreamID(7)
	require.True(t, ok)
	require.Equal(t, "shows", streamID)

	specID, ok := streams.SpecID("clicks")
	require.True(t, ok)
	require.Equal(t, clicksSpecID, specID)

	stream, ok := streams.StreamBySpecID(clicksSpecID)
	require.True(t, ok)
	_, ok = stream.Schema.FindColumn("url")
	require.True(t, ok)
}

func TestStreamSpecsFromProtoRejectsMalformedSchema(t *testing.T) {
	_, err := streamSpecsFromProto([]*companion.TStream{{
		StreamId:     proto.String("clicks"),
		StreamSpecId: proto.Int64(clicksSpecID),
		Schema:       []byte("{not a schema"),
	}})
	require.ErrorContains(t, err, "clicks")
}

func TestJobFromProtoParsesJobInfo(t *testing.T) {
	job := protoJob(t)

	require.Equal(t, protoJobID, job.ID())
	require.Equal(t, "counter", job.ComputationID())
	require.Equal(t, []string{"counters", "windows"}, job.InternalStateNames())
	require.Equal(t, []string{"/aux/state", "/state"}, job.ExternalStateNames())
	require.Equal(t, []string{"/joined"}, job.JoinedExternalStateNames())

	require.Equal(t, []string{"hash", "user_id"}, columnNames(job.GroupBySchema()))

	_, ok := job.StreamSpecs().StreamBySpecID(clicksSpecID)
	require.True(t, ok)

	var windowSize int
	require.NoError(t, job.DynamicParameters().Get("window_size", &windowSize))
	require.Equal(t, 200, windowSize)
}

func TestPutJobFromProtoReadsRequestFields(t *testing.T) {
	job, err := putJobFromProto(&companion.TReqPutJob{
		RequestId:     misc.NewProtoFromGUID(guid.FromHalves(7, 8)),
		JobId:         misc.NewProtoFromGUID(protoJobID),
		ComputationId: proto.String("counter"),
		JobInfo:       protoJobInfo(t),
	})
	require.NoError(t, err)

	require.Equal(t, protoJobID, job.ID())
	require.Equal(t, "counter", job.ComputationID())
}

func TestProcessBatchFromProtoDecodesMessages(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	_, batch := processBatch(t, &companion.TReqProcessBatch{
		Messages: []*companion.TReqProcessBatch_TExtendedMessage{clickMessage(t, "m-1", "http://a", key)},
	})

	require.Len(t, batch.Messages, 1)
	msg := batch.Messages[0]

	require.Equal(t, "m-1", msg.ID)
	require.Equal(t, uint64(1700), msg.SystemTimestamp)
	require.Equal(t, uint64(1600), msg.EventTimestamp)
	require.Equal(t, "clicks", msg.StreamID)
	require.Equal(t, clicksSpecID, msg.StreamSpecID)

	url, err := msg.Payload.String("url")
	require.NoError(t, err)
	require.Equal(t, "http://a", url)

	userID, err := msg.Key.String("user_id")
	require.NoError(t, err)
	require.Equal(t, "user-1", userID)
}

func TestProcessBatchFromProtoKeepsMessageOfUnknownStreamSpec(t *testing.T) {
	message := clickMessage(t, "m-1", "http://a", batchKey(t, 17, "user-1"))
	message.Message.StreamSpecId = proto.Int64(unknownSpecID)

	_, batch := processBatch(t, &companion.TReqProcessBatch{
		Messages: []*companion.TReqProcessBatch_TExtendedMessage{message},
	})

	msg := batch.Messages[0]
	require.Empty(t, msg.StreamID)
	require.Equal(t, unknownSpecID, msg.StreamSpecID)

	value, ok := msg.Payload.valueByID(0)
	require.True(t, ok)
	require.Equal(t, []byte("http://a"), value.Bytes())
}

func TestProcessBatchFromProtoOverridesStreamsOfSourceComputation(t *testing.T) {
	runtime, batch := processBatch(t, &companion.TReqProcessBatch{
		Streams: []*companion.TStream{protoStream(t, "ingested", unknownSpecID, streamSchema("url"))},
		Messages: []*companion.TReqProcessBatch_TExtendedMessage{
			clickMessage(t, "m-1", "http://a", batchKey(t, 17, "user-1")),
		},
	})

	_, ok := runtime.StreamSpecs().Stream("clicks")
	require.False(t, ok)
	_, ok = runtime.StreamSpecs().Stream("ingested")
	require.True(t, ok)

	require.Empty(t, batch.Messages[0].StreamID)

	message := clickMessage(t, "m-2", "http://b", batchKey(t, 17, "user-1"))
	message.Message.StreamSpecId = proto.Int64(unknownSpecID)
	_, batch = processBatch(t, &companion.TReqProcessBatch{
		Streams:  []*companion.TStream{protoStream(t, "ingested", unknownSpecID, streamSchema("url"))},
		Messages: []*companion.TReqProcessBatch_TExtendedMessage{message},
	})
	require.Equal(t, "ingested", batch.Messages[0].StreamID)
}

func TestProcessBatchFromProtoDecodesTimersAndVisits(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	_, batch := processBatch(t, &companion.TReqProcessBatch{
		Timers: []*common.TTimer{{
			MessageId:        proto.String("t-1"),
			SystemTimestamp:  proto.Uint64(1700),
			EventTimestamp:   proto.Uint64(1600),
			StreamId:         []byte("windows"),
			Key:              encodeRow(t, key),
			TriggerTimestamp: proto.Uint64(1800),
		}},
		Visits: []*common.TVisit{{
			MessageId:       proto.String("v-1"),
			SystemTimestamp: proto.Uint64(1750),
			StreamId:        []byte("visitor"),
			Key:             encodeRow(t, key),
		}},
	})

	require.Len(t, batch.Timers, 1)
	timer := batch.Timers[0]
	require.Equal(t, "t-1", timer.ID)
	require.Equal(t, uint64(1800), timer.TriggerTimestamp)
	require.Equal(t, "windows", timer.StreamID)
	require.Equal(t, NoStreamSpecID, timer.StreamSpecID)

	require.Len(t, batch.Visits, 1)
	visit := batch.Visits[0]
	require.Equal(t, "v-1", visit.ID)
	require.Equal(t, uint64(1750), visit.SystemTimestamp)
	require.Zero(t, visit.EventTimestamp)
	require.Equal(t, NoStreamSpecID, visit.StreamSpecID)

	userID, err := visit.Key.String("user_id")
	require.NoError(t, err)
	require.Equal(t, "user-1", userID)
}

func TestProcessBatchFromProtoComputesMinWatermark(t *testing.T) {
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{
		Watermarks: []*companion.TWatermark{
			{StreamId: proto.String("clicks"), Watermark: proto.Uint64(500)},
			{StreamId: proto.String("shows"), Watermark: proto.Uint64(300)},
		},
	})

	require.Equal(t, uint64(300), runtime.MinWatermark())

	watermark, ok := runtime.Watermark("clicks")
	require.True(t, ok)
	require.Equal(t, uint64(500), watermark)

	_, ok = runtime.Watermark("unknown")
	require.False(t, ok)

	runtime, _ = processBatch(t, &companion.TReqProcessBatch{})
	require.Zero(t, runtime.MinWatermark())
}

func TestProcessBatchFromProtoLoadsStatesWithoutMarkingThemModified(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	row := externalStateRow(t, 5, "gold")

	runtime, _ := processBatch(t, &companion.TReqProcessBatch{
		InternalStates: []*companion.TState{{
			Name:       proto.String("counters"),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, []byte("stored"), false)},
		}},
		ExternalStates: []*companion.TState{{
			Name:       proto.String("/state"),
			Schema:     encodeSchema(t, externalStateSchema()),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, encodeRow(t, row), false)},
		}},
	})

	internal, err := runtime.InternalState("counters")
	require.NoError(t, err)
	loaded, ok := internal.Get(key)
	require.True(t, ok)
	require.Equal(t, []byte("stored"), loaded.Data)

	external, err := runtime.ExternalState("/state")
	require.NoError(t, err)
	storedRow, ok := external.Get(key)
	require.True(t, ok)
	count, err := storedRow.Value.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(5), count)

	data, err := ResponseDataToProto(runtime, nil)
	require.NoError(t, err)
	require.Empty(t, data.GetInternalStates())
	require.Empty(t, data.GetExternalStates())
}

func TestProcessBatchFromProtoDistinguishesNullAndEmptyExternalStateRows(t *testing.T) {
	absentKey := batchKey(t, 17, "absent")
	emptyKey := batchKey(t, 18, "empty")
	emptyRow, err := wire.MarshalRowProto(wire.Row{})
	require.NoError(t, err)

	runtime, batch := processBatch(t, &companion.TReqProcessBatch{
		Messages: []*companion.TReqProcessBatch_TExtendedMessage{
			clickMessage(t, "m-1", "http://a", absentKey),
			clickMessage(t, "m-2", "http://b", emptyKey),
		},
		ExternalStates: []*companion.TState{{
			Name:   proto.String("/state"),
			Schema: encodeSchema(t, externalStateSchema()),
			StateItems: []*companion.TStateItem{
				protoStateItem(t, absentKey, nil, false),
				protoStateItem(t, emptyKey, emptyRow, false),
			},
		}},
	})

	absent, err := OpenExternalState(runtime, "/state", batch.Messages[0])
	require.NoError(t, err)
	_, ok := absent.Get()
	require.False(t, ok)
	fallback := externalStateRow(t, 7, "fallback")
	require.Equal(t, fallback, absent.Or(fallback))
	value := externalStateValue{Count: 7, Label: "unchanged"}
	exists, err := absent.ConvertTo(&value)
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, externalStateValue{Count: 7, Label: "unchanged"}, value)

	present, err := OpenExternalState(runtime, "/state", batch.Messages[1])
	require.NoError(t, err)
	row, ok := present.Get()
	require.True(t, ok)
	require.NotNil(t, row.Row())
	require.Empty(t, row.Row())
}

func TestProcessBatchFromProtoLoadsStateUnderTheInputKey(t *testing.T) {
	key := batchKey(t, 17, "user-1")

	runtime, batch := processBatch(t, &companion.TReqProcessBatch{
		Messages: []*companion.TReqProcessBatch_TExtendedMessage{clickMessage(t, "m-1", "http://a", key)},
		InternalStates: []*companion.TState{{
			Name:       proto.String("counters"),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, []byte("stored"), false)},
		}},
	})

	state, err := OpenRawState(runtime, "counters", batch.Messages[0])
	require.NoError(t, err)
	data, ok := state.Get()
	require.True(t, ok)
	require.Equal(t, []byte("stored"), data)
}

func TestProcessBatchFromProtoRejectsExternalStateWithoutSchema(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	_, _, err := processBatchFromProto(&companion.TReqProcessBatch{
		RequestId: misc.NewProtoFromGUID(guid.FromHalves(7, 8)),
		JobId:     misc.NewProtoFromGUID(protoJobID),
		ExternalStates: []*companion.TState{{
			Name:       proto.String("/state"),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, encodeRow(t, key), false)},
		}},
	}, protoJob(t))

	require.ErrorIs(t, err, ErrNoStateSchema)
}

func TestProcessBatchFromProtoRejectsUndeclaredState(t *testing.T) {
	key := batchKey(t, 17, "user-1")

	_, _, err := processBatchFromProto(&companion.TReqProcessBatch{
		InternalStates: []*companion.TState{{
			Name:       proto.String("unknown"),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, []byte("stored"), false)},
		}},
	}, protoJob(t))
	require.ErrorIs(t, err, ErrUnknownState)

	_, _, err = processBatchFromProto(&companion.TReqProcessBatch{
		JoinedExternalStates: []*companion.TState{{
			Name:       proto.String("/state"),
			Schema:     encodeSchema(t, externalStateSchema()),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, encodeRow(t, key), false)},
		}},
	}, protoJob(t))
	require.ErrorIs(t, err, ErrUnknownState)
}

func TestProcessBatchFromProtoReportsMalformedRows(t *testing.T) {
	message := clickMessage(t, "m-1", "http://a", batchKey(t, 17, "user-1"))
	message.Key = []byte{0xff}

	_, _, err := processBatchFromProto(&companion.TReqProcessBatch{
		RequestId: misc.NewProtoFromGUID(guid.FromHalves(7, 8)),
		JobId:     misc.NewProtoFromGUID(protoJobID),
		Messages:  []*companion.TReqProcessBatch_TExtendedMessage{message},
	}, protoJob(t))

	require.ErrorContains(t, err, "m-1")
	require.ErrorContains(t, err, protoJobID.String())
}

func TestJoinedExternalStateIsNeverMapped(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{
		JoinedExternalStates: []*companion.TState{{
			Name:       proto.String("/joined"),
			Schema:     encodeSchema(t, externalStateSchema()),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, encodeRow(t, externalStateRow(t, 5, "gold")), false)},
		}},
	})

	joined, err := runtime.JoinedExternalState("/joined")
	require.NoError(t, err)
	require.Equal(t, 1, joined.Len())

	require.NoError(t, joined.Set(key, ExternalState{Value: externalStateRow(t, 6, "platinum")}))

	data, err := ResponseDataToProto(runtime, nil)
	require.NoError(t, err)
	require.Empty(t, data.GetExternalStates())
}

func TestResponseDataToProtoRendersGroup(t *testing.T) {
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{})

	b, err := runtime.MessageBuilder("clicks")
	require.NoError(t, err)
	b.SetSystemTimestamp(1700)
	msg, err := b.Set("url", "http://a").Finish()
	require.NoError(t, err)

	data, err := ResponseDataToProto(runtime, []OutputGroup{{
		ParentIDs: []string{"m-1", "m-2"},
		Messages:  []Message{msg},
		Timers:    []TimerRequest{{TriggerTimestamp: 1800, EventTimestamp: 1600, StreamID: "windows"}},
	}})
	require.NoError(t, err)

	require.Len(t, data.GetOutput(), 1)
	group := data.GetOutput()[0]
	require.Equal(t, [][]byte{[]byte("m-1"), []byte("m-2")}, group.GetParentIds())

	require.Len(t, group.GetMessages(), 1)
	protoMessage := group.GetMessages()[0]
	require.Equal(t, clicksSpecID, protoMessage.GetStreamSpecId())
	require.Equal(t, uint64(1700), protoMessage.GetSystemTimestamp())
	require.Empty(t, protoMessage.GetMessageId())
	require.Nil(t, protoMessage.EventTimestamp)

	row, err := wire.UnmarshalRowProto(protoMessage.GetPayload())
	require.NoError(t, err)
	url, err := NewPayload(row, streamSchema("url")).String("url")
	require.NoError(t, err)
	require.Equal(t, "http://a", url)

	require.Len(t, group.GetTimers(), 1)
	protoTimer := group.GetTimers()[0]
	require.Equal(t, uint64(1800), protoTimer.GetTriggerTimestamp())
	require.Equal(t, uint64(1600), protoTimer.GetEventTimestamp())
	require.Equal(t, []byte("windows"), protoTimer.GetStreamId())
}

func TestResponseDataToProtoOmitsUnsetTimerStream(t *testing.T) {
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{})

	data, err := ResponseDataToProto(runtime, []OutputGroup{{
		ParentIDs: []string{"m-1"},
		Timers:    []TimerRequest{{TriggerTimestamp: 1800}},
	}})
	require.NoError(t, err)

	protoTimer := data.GetOutput()[0].GetTimers()[0]
	require.Nil(t, protoTimer.StreamId)
	require.Nil(t, protoTimer.EventTimestamp)
}

func TestResponseDataToProtoCopiesDistributeFlags(t *testing.T) {
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{})

	b, err := runtime.MessageBuilder("clicks")
	require.NoError(t, err)
	first, err := b.Finish()
	require.NoError(t, err)
	second, err := b.Finish()
	require.NoError(t, err)
	messages := []Message{first, second}

	data, err := ResponseDataToProto(runtime, []OutputGroup{{
		ParentIDs: []string{"m-1"},
		Messages:  messages,
	}})
	require.NoError(t, err)
	require.Empty(t, data.GetOutput()[0].GetDistribute())

	data, err = ResponseDataToProto(runtime, []OutputGroup{{
		ParentIDs:  []string{"m-1"},
		Messages:   messages,
		Distribute: []bool{true, false},
	}})
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, data.GetOutput()[0].GetDistribute())
}

func TestResponseDataToProtoRejectsGroupWithoutParents(t *testing.T) {
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{})

	_, err := ResponseDataToProto(runtime, []OutputGroup{{
		Timers: []TimerRequest{{TriggerTimestamp: 1800}},
	}})
	require.ErrorIs(t, err, errNoParentIDs)
}

func TestResponseDataToProtoRejectsMessageOnUnknownStream(t *testing.T) {
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{})

	_, err := ResponseDataToProto(runtime, []OutputGroup{{
		ParentIDs: []string{"m-1"},
		Messages:  []Message{{Meta: Meta{StreamID: "shows"}}},
	}})
	require.ErrorIs(t, err, ErrUnknownStream)
}

func TestResponseDataToProtoRejectsEmptyMessagePayload(t *testing.T) {
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{})

	_, err := ResponseDataToProto(runtime, []OutputGroup{{
		ParentIDs: []string{"m-1"},
		Messages:  []Message{{Meta: Meta{StreamID: "clicks"}}},
	}})
	require.ErrorIs(t, err, errEmptyMessagePayload)
	require.ErrorContains(t, err, "output group 0")
	require.ErrorContains(t, err, "message 0")
	require.ErrorContains(t, err, `stream "clicks"`)
}

func TestResponseDataToProtoSendsOnlyModifiedStates(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	other := batchKey(t, 18, "user-2")

	runtime, _ := processBatch(t, &companion.TReqProcessBatch{
		InternalStates: []*companion.TState{
			{
				Name: proto.String("counters"),
				StateItems: []*companion.TStateItem{
					protoStateItem(t, key, []byte("stored"), false),
					protoStateItem(t, other, []byte("stored"), false),
				},
			},
			{
				Name:       proto.String("windows"),
				StateItems: []*companion.TStateItem{protoStateItem(t, key, []byte("stored"), false)},
			},
		},
	})

	counters, err := runtime.InternalState("counters")
	require.NoError(t, err)
	require.NoError(t, counters.Set(key, InternalState{Data: []byte("written")}))

	data, err := ResponseDataToProto(runtime, nil)
	require.NoError(t, err)

	require.Len(t, data.GetInternalStates(), 1)
	state := data.GetInternalStates()[0]
	require.Equal(t, "counters", state.GetName())

	require.Len(t, state.GetStateItems(), 1)
	item := state.GetStateItems()[0]
	require.Equal(t, encodeRow(t, key), item.GetKey())
	require.False(t, item.GetReset_())
	require.Equal(t, []byte("written"), item.GetState())
}

func TestResponseDataToProtoRendersClearedStateAsReset(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{
		InternalStates: []*companion.TState{{
			Name:       proto.String("counters"),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, []byte("stored"), false)},
		}},
		ExternalStates: []*companion.TState{{
			Name:       proto.String("/state"),
			Schema:     encodeSchema(t, externalStateSchema()),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, encodeRow(t, externalStateRow(t, 5, "gold")), false)},
		}},
	})

	counters, err := runtime.InternalState("counters")
	require.NoError(t, err)
	require.NoError(t, counters.Clear(key))

	external, err := runtime.ExternalState("/state")
	require.NoError(t, err)
	require.NoError(t, external.Clear(key))

	data, err := ResponseDataToProto(runtime, nil)
	require.NoError(t, err)

	internalItem := data.GetInternalStates()[0].GetStateItems()[0]
	require.True(t, internalItem.GetReset_())
	require.Nil(t, internalItem.State)

	externalItem := data.GetExternalStates()[0].GetStateItems()[0]
	require.True(t, externalItem.GetReset_())
	require.Nil(t, externalItem.State)
}

func TestResponseDataToProtoRejectsEmptyInternalStateValue(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{})

	counters, err := runtime.InternalState("counters")
	require.NoError(t, err)
	require.NoError(t, counters.Set(key, InternalState{}))

	_, err = ResponseDataToProto(runtime, nil)
	require.ErrorIs(t, err, ErrEmptyStateValue)
	require.ErrorContains(t, err, "counters")
}

func TestResponseDataToProtoDistinguishesNilAndEmptyExternalStateRows(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{
		ExternalStates: []*companion.TState{{
			Name:       proto.String("/state"),
			Schema:     encodeSchema(t, externalStateSchema()),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, encodeRow(t, externalStateRow(t, 5, "gold")), false)},
		}},
	})

	external, err := runtime.ExternalState("/state")
	require.NoError(t, err)
	require.NoError(t, external.Set(key, ExternalState{}))

	_, err = ResponseDataToProto(runtime, nil)
	require.ErrorIs(t, err, ErrEmptyStateValue)
	require.ErrorContains(t, err, "/state")

	empty := NewPayload(wire.Row{}, externalStateSchema())
	require.NoError(t, external.Set(key, ExternalState{Value: empty}))
	data, err := ResponseDataToProto(runtime, nil)
	require.NoError(t, err)

	row, err := wire.UnmarshalRowProto(data.GetExternalStates()[0].GetStateItems()[0].GetState())
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Empty(t, row)
}

func TestResponseDataToProtoWritesExternalStateRows(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	runtime, _ := processBatch(t, &companion.TReqProcessBatch{
		ExternalStates: []*companion.TState{{
			Name:       proto.String("/state"),
			Schema:     encodeSchema(t, externalStateSchema()),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, encodeRow(t, externalStateRow(t, 5, "gold")), false)},
		}},
	})

	external, err := runtime.ExternalState("/state")
	require.NoError(t, err)
	require.NoError(t, external.Set(key, ExternalState{Value: externalStateRow(t, 6, "platinum")}))

	data, err := ResponseDataToProto(runtime, nil)
	require.NoError(t, err)

	require.Len(t, data.GetExternalStates(), 1)
	require.Equal(t, "/state", data.GetExternalStates()[0].GetName())

	item := data.GetExternalStates()[0].GetStateItems()[0]
	row, err := wire.UnmarshalRowProto(item.GetState())
	require.NoError(t, err)

	count, err := NewPayload(row, externalStateSchema()).Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(6), count)
}

func TestProcessBatchResponseSerializes(t *testing.T) {
	key := batchKey(t, 17, "user-1")
	requestID := guid.FromHalves(0xfeedfacecafebeef, 3)

	req := &companion.TReqProcessBatch{
		RequestId:     misc.NewProtoFromGUID(requestID),
		JobId:         misc.NewProtoFromGUID(protoJobID),
		ComputationId: proto.String("counter"),
		Messages:      []*companion.TReqProcessBatch_TExtendedMessage{clickMessage(t, "m-1", "http://a", key)},
		InternalStates: []*companion.TState{{
			Name:       proto.String("counters"),
			StateItems: []*companion.TStateItem{protoStateItem(t, key, []byte("stored"), false)},
		}},
	}

	runtime, batch := processBatch(t, req)

	computation := NewRowComputation("counter", RowFunc(
		func(_ context.Context, rt Runtime, msg ExtendedMessage, out OutputCollector) error {
			state, err := OpenRawState(rt, "counters", msg)
			if err != nil {
				return err
			}
			if err := state.Set(append(state.Or(nil), '!')); err != nil {
				return err
			}

			b, err := rt.MessageBuilder("clicks")
			if err != nil {
				return err
			}
			click, err := b.Set("url", "http://b").Finish()
			if err != nil {
				return err
			}
			out.AddMessage(click)
			out.AddTimer(TimerRequest{TriggerTimestamp: 1800})
			return nil
		}))

	results, err := computation.Process(context.Background(), runtime, batch)
	require.NoError(t, err)

	data, err := ResponseDataToProto(runtime, results)
	require.NoError(t, err)

	rsp := &companion.TRspProcessBatch{
		RequestId: misc.NewProtoFromGUID(requestID),
		JobId:     misc.NewProtoFromGUID(protoJobID),
		Data:      data,
		Metrics: &companion.TResponseMetrics{
			AllocatedBytes: proto.Int64(1024),
			CpuTimeNs:      proto.Int64(2048),
		},
		Status: companion.EResponseStatus_RS_OK.Enum(),
	}

	encoded, err := proto.Marshal(rsp)
	require.NoError(t, err)

	var decoded companion.TRspProcessBatch
	require.NoError(t, proto.Unmarshal(encoded, &decoded))

	require.Equal(t, requestID, misc.NewGUIDFromProto(decoded.GetRequestId()))
	require.Equal(t, [][]byte{[]byte("m-1")}, decoded.GetData().GetOutput()[0].GetParentIds())
	require.Equal(t, []byte("stored!"), decoded.GetData().GetInternalStates()[0].GetStateItems()[0].GetState())
}

func columnNames(s Schema) []string {
	names := make([]string, 0, s.Len())
	for _, column := range s.Columns() {
		names = append(names, column.Name)
	}
	return names
}
