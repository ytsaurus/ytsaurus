package flow

import (
	"slices"

	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/proto/flow/common"
	"go.ytsaurus.tech/yt/go/proto/flow/companion"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/wire"
	"go.ytsaurus.tech/yt/go/yson"
)

var (
	errNoParentIDs         = xerrors.NewSentinel("output group has no parent ids")
	errEmptyMessagePayload = xerrors.NewSentinel("empty message payload")

	// ErrEmptyStateValue reports a state written as empty bytes rather than cleared.
	ErrEmptyStateValue = xerrors.NewSentinel("empty state value")
)

func streamSpecsFromProto(protoStreams []*companion.TStream) (StreamSpecs, error) {
	specIDs := make(map[string]int64, len(protoStreams))
	streams := make([]Stream, 0, len(protoStreams))

	for _, protoStream := range protoStreams {
		streamSchema, err := schemaFromProto(protoStream.GetSchema())
		if err != nil {
			return StreamSpecs{}, xerrors.Errorf("stream %q: %w", protoStream.GetStreamId(), err)
		}
		specIDs[protoStream.GetStreamId()] = protoStream.GetStreamSpecId()
		streams = append(streams, NewStream(protoStream.GetStreamId(), streamSchema))
	}

	return NewStreamSpecs(specIDs, streams), nil
}

func jobFromProto(id guid.GUID, computationID string, info *companion.TJobInfo) (*Job, error) {
	streams, err := streamSpecsFromProto(info.GetStreams())
	if err != nil {
		return nil, xerrors.Errorf("flow: job %v: %w", id, err)
	}
	return NewJob(id, computationID, streams, info.GetSpec(), info.GetDynamicSpec())
}

func putJobFromProto(req *companion.TReqPutJob) (*Job, error) {
	return jobFromProto(misc.NewGUIDFromProto(req.GetJobId()), req.GetComputationId(), req.GetJobInfo())
}

func processBatchFromProto(req *companion.TReqProcessBatch, job *Job) (*RequestRuntime, Batch, error) {
	runtime, batch, err := readProcessBatch(req, job)
	if err != nil {
		return nil, Batch{}, xerrors.Errorf("flow: request %v of job %v: %w",
			misc.NewGUIDFromProto(req.GetRequestId()), misc.NewGUIDFromProto(req.GetJobId()), err)
	}
	return runtime, batch, nil
}

func readProcessBatch(req *companion.TReqProcessBatch, job *Job) (*RequestRuntime, Batch, error) {
	runtime := NewRequestRuntime(job)

	// Source input streams are request-scoped.
	if len(req.GetStreams()) > 0 {
		streams, err := streamSpecsFromProto(req.GetStreams())
		if err != nil {
			return nil, Batch{}, err
		}
		runtime.SetStreamSpecs(streams)
	}

	for _, watermark := range req.GetWatermarks() {
		runtime.SetWatermark(watermark.GetStreamId(), watermark.GetWatermark())
	}

	keySchema := job.GroupBySchema()
	streams := runtime.StreamSpecs()

	batch := Batch{
		Messages: make([]ExtendedMessage, 0, len(req.GetMessages())),
		Timers:   make([]Timer, 0, len(req.GetTimers())),
		Visits:   make([]Visit, 0, len(req.GetVisits())),
	}

	for _, protoMessage := range req.GetMessages() {
		msg, err := extendedMessageFromProto(protoMessage, keySchema, streams)
		if err != nil {
			return nil, Batch{}, err
		}
		batch.Messages = append(batch.Messages, msg)
	}

	for _, protoTimer := range req.GetTimers() {
		timer, err := timerFromProto(protoTimer, keySchema)
		if err != nil {
			return nil, Batch{}, err
		}
		batch.Timers = append(batch.Timers, timer)
	}

	for _, protoVisit := range req.GetVisits() {
		visit, err := visitFromProto(protoVisit, keySchema)
		if err != nil {
			return nil, Batch{}, err
		}
		batch.Visits = append(batch.Visits, visit)
	}

	if err := loadStates(runtime, req, keySchema); err != nil {
		return nil, Batch{}, err
	}

	return runtime, batch, nil
}

// ResponseDataToProto renders computation output for the worker.
func ResponseDataToProto(runtime *RequestRuntime, results []OutputGroup) (*companion.TResponseData, error) {
	data := &companion.TResponseData{
		Output: make([]*companion.TResponseData_TGroup, 0, len(results)),
	}

	streams := runtime.StreamSpecs()
	for i, result := range results {
		group, err := groupToProto(result, streams)
		if err != nil {
			return nil, xerrors.Errorf("flow: output group %d: %w", i, err)
		}
		data.Output = append(data.Output, group)
	}

	for holder := range runtime.ModifiedInternalStates() {
		state, err := internalStateToProto(holder)
		if err != nil {
			return nil, err
		}
		data.InternalStates = append(data.InternalStates, state)
	}

	for holder := range runtime.ModifiedExternalStates() {
		state, err := externalStateToProto(holder)
		if err != nil {
			return nil, err
		}
		data.ExternalStates = append(data.ExternalStates, state)
	}

	return data, nil
}

func extendedMessageFromProto(
	protoMessage *companion.TReqProcessBatch_TExtendedMessage,
	keySchema Schema,
	streams StreamSpecs,
) (ExtendedMessage, error) {
	msg, err := messageFromProto(protoMessage.GetMessage(), streams)
	if err != nil {
		return ExtendedMessage{}, err
	}

	key, err := payloadFromProto(protoMessage.GetKey(), keySchema)
	if err != nil {
		return ExtendedMessage{}, xerrors.Errorf("message %q key: %w", msg.ID, err)
	}

	return ExtendedMessage{Message: msg, Key: key}, nil
}

// Messages from older schema revisions may no longer resolve to a stream.
// Their payload remains accessible by column id.
func messageFromProto(protoMessage *common.TMessage, streams StreamSpecs) (Message, error) {
	specID := protoMessage.GetStreamSpecId()
	streamID, _ := streams.StreamID(specID)

	var payloadSchema Schema
	if stream, ok := streams.StreamBySpecID(specID); ok {
		payloadSchema = stream.Schema
	}

	payload, err := payloadFromProto(protoMessage.GetPayload(), payloadSchema)
	if err != nil {
		return Message{}, xerrors.Errorf("message %q payload: %w", protoMessage.GetMessageId(), err)
	}

	return Message{
		Meta: Meta{
			ID:              protoMessage.GetMessageId(),
			EventTimestamp:  protoMessage.GetEventTimestamp(),
			SystemTimestamp: protoMessage.GetSystemTimestamp(),
			StreamID:        streamID,
			StreamSpecID:    specID,
		},
		Payload: payload,
	}, nil
}

func messageToProto(msg Message, streams StreamSpecs) (*common.TMessage, error) {
	specID, ok := streams.SpecID(msg.StreamID)
	if !ok {
		return nil, xerrors.Errorf("%w: %q", ErrUnknownStream, msg.StreamID)
	}

	row := msg.Payload.row
	if row == nil {
		return nil, xerrors.Errorf("%w on stream %q", errEmptyMessagePayload, msg.StreamID)
	}

	payload, err := wire.MarshalRowProto(row)
	if err != nil {
		return nil, xerrors.Errorf("payload of a message on stream %q: %w", msg.StreamID, err)
	}

	protoMessage := &common.TMessage{
		// The worker replaces this required placeholder.
		MessageId:       proto.String(msg.ID),
		SystemTimestamp: proto.Uint64(msg.SystemTimestamp),
		Payload:         payload,
		StreamSpecId:    proto.Int64(specID),
	}
	if msg.EventTimestamp > 0 {
		protoMessage.EventTimestamp = proto.Uint64(msg.EventTimestamp)
	}
	return protoMessage, nil
}

func timerFromProto(protoTimer *common.TTimer, keySchema Schema) (Timer, error) {
	key, err := payloadFromProto(protoTimer.GetKey(), keySchema)
	if err != nil {
		return Timer{}, xerrors.Errorf("timer %q key: %w", protoTimer.GetMessageId(), err)
	}

	return Timer{
		Meta: Meta{
			ID:              protoTimer.GetMessageId(),
			EventTimestamp:  protoTimer.GetEventTimestamp(),
			SystemTimestamp: protoTimer.GetSystemTimestamp(),
			StreamID:        string(protoTimer.GetStreamId()),
			StreamSpecID:    NoStreamSpecID,
		},
		TriggerTimestamp: protoTimer.GetTriggerTimestamp(),
		Key:              key,
	}, nil
}

func newTimerToProto(timer TimerRequest) *companion.TNewTimer {
	protoTimer := &companion.TNewTimer{
		TriggerTimestamp: proto.Uint64(timer.TriggerTimestamp),
	}
	if timer.EventTimestamp > 0 {
		protoTimer.EventTimestamp = proto.Uint64(timer.EventTimestamp)
	}
	// An omitted stream id selects the only timer stream.
	if timer.StreamID != "" {
		protoTimer.StreamId = []byte(timer.StreamID)
	}
	return protoTimer
}

func visitFromProto(protoVisit *common.TVisit, keySchema Schema) (Visit, error) {
	key, err := payloadFromProto(protoVisit.GetKey(), keySchema)
	if err != nil {
		return Visit{}, xerrors.Errorf("visit %q key: %w", protoVisit.GetMessageId(), err)
	}

	return NewVisit(Meta{
		ID:              protoVisit.GetMessageId(),
		EventTimestamp:  protoVisit.GetEventTimestamp(),
		SystemTimestamp: protoVisit.GetSystemTimestamp(),
		StreamID:        string(protoVisit.GetStreamId()),
	}, key), nil
}

func groupToProto(result OutputGroup, streams StreamSpecs) (*companion.TResponseData_TGroup, error) {
	if len(result.ParentIDs) == 0 {
		return nil, errNoParentIDs
	}

	group := &companion.TResponseData_TGroup{
		Messages:   make([]*common.TMessage, 0, len(result.Messages)),
		Timers:     make([]*companion.TNewTimer, 0, len(result.Timers)),
		ParentIds:  make([][]byte, 0, len(result.ParentIDs)),
		Distribute: slices.Clone(result.Distribute),
	}

	for _, parentID := range result.ParentIDs {
		group.ParentIds = append(group.ParentIds, []byte(parentID))
	}

	for i, msg := range result.Messages {
		protoMessage, err := messageToProto(msg, streams)
		if err != nil {
			return nil, xerrors.Errorf("message %d: %w", i, err)
		}
		group.Messages = append(group.Messages, protoMessage)
	}

	for _, timer := range result.Timers {
		group.Timers = append(group.Timers, newTimerToProto(timer))
	}

	return group, nil
}

func loadStates(runtime *RequestRuntime, req *companion.TReqProcessBatch, keySchema Schema) error {
	for _, protoState := range req.GetInternalStates() {
		for _, item := range protoState.GetStateItems() {
			key, err := payloadFromProto(item.GetKey(), keySchema)
			if err != nil {
				return xerrors.Errorf("internal state %q: key: %w", protoState.GetName(), err)
			}
			value := InternalState{Reset: item.GetReset_(), Data: item.GetState()}
			if err := runtime.LoadInternalState(protoState.GetName(), key, value); err != nil {
				return err
			}
		}
	}

	if err := loadExternalStates(req.GetExternalStates(), keySchema, runtime.LoadExternalState); err != nil {
		return err
	}

	return loadExternalStates(req.GetJoinedExternalStates(), keySchema, runtime.LoadJoinedExternalState)
}

func loadExternalStates(
	protoStates []*companion.TState,
	keySchema Schema,
	load func(name string, stateSchema Schema, key Payload, value ExternalState) error,
) error {
	for _, protoState := range protoStates {
		stateSchema, err := schemaFromProto(protoState.GetSchema())
		if err != nil {
			return xerrors.Errorf("external state %q: %w", protoState.GetName(), err)
		}

		for _, item := range protoState.GetStateItems() {
			key, err := payloadFromProto(item.GetKey(), keySchema)
			if err != nil {
				return xerrors.Errorf("external state %q: key: %w", protoState.GetName(), err)
			}

			value := ExternalState{Reset: item.GetReset_()}
			if !value.Reset {
				if value.Value, err = payloadFromProto(item.GetState(), stateSchema); err != nil {
					return xerrors.Errorf("external state %q: value: %w", protoState.GetName(), err)
				}
			}

			if err := load(protoState.GetName(), stateSchema, key, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func internalStateToProto(holder *StatesHolder[InternalState]) (*companion.TState, error) {
	state := &companion.TState{Name: proto.String(holder.Name())}

	for key, value := range holder.Modified() {
		item, err := stateItemToProto(holder.Name(), key, value.Reset)
		if err != nil {
			return nil, err
		}
		if !value.Reset {
			// Empty data is neither a value nor a reset.
			if len(value.Data) == 0 {
				return nil, xerrors.Errorf("flow: internal state %q: %w", holder.Name(), ErrEmptyStateValue)
			}
			item.State = value.Data
		}
		state.StateItems = append(state.StateItems, item)
	}

	return state, nil
}

func externalStateToProto(holder *StatesHolder[ExternalState]) (*companion.TState, error) {
	state := &companion.TState{Name: proto.String(holder.Name())}

	for key, value := range holder.Modified() {
		item, err := stateItemToProto(holder.Name(), key, value.Reset)
		if err != nil {
			return nil, err
		}
		if !value.Reset {
			row := value.Value.row
			if row == nil {
				return nil, xerrors.Errorf("flow: external state %q: %w", holder.Name(), ErrEmptyStateValue)
			}
			encoded, err := wire.MarshalRowProto(row)
			if err != nil {
				return nil, xerrors.Errorf("flow: external state %q: value: %w", holder.Name(), err)
			}
			item.State = encoded
		}
		state.StateItems = append(state.StateItems, item)
	}

	return state, nil
}

func stateItemToProto(name string, key Payload, reset bool) (*companion.TStateItem, error) {
	encoded, err := wire.MarshalRowProto(key.row)
	if err != nil {
		return nil, xerrors.Errorf("flow: state %q: key: %w", name, err)
	}
	return &companion.TStateItem{Key: encoded, Reset_: proto.Bool(reset)}, nil
}

func payloadFromProto(data []byte, s Schema) (Payload, error) {
	row, err := wire.UnmarshalRowProto(data)
	if err != nil {
		return Payload{}, err
	}
	return Payload{row: row, schema: s}, nil
}

func schemaFromProto(data []byte) (Schema, error) {
	if len(data) == 0 {
		return Schema{}, nil
	}
	var table schema.Schema
	if err := yson.Unmarshal(data, &table); err != nil {
		return Schema{}, xerrors.Errorf("parsing schema: %w", err)
	}
	s := NewSchema(table)
	if err := s.validate(); err != nil {
		return Schema{}, err
	}
	return s, nil
}
