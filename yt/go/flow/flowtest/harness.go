// Package flowtest tests Flow computations without a cluster.
package flowtest

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/wire"
	"go.ytsaurus.tech/yt/go/yson"
)

// Row is a set of column values keyed by column name.
type Row map[string]any

// Options configures a Harness.
type Options struct {
	// Streams contains the computation streams.
	Streams map[string]flow.Schema

	// KeySchema describes grouping keys.
	KeySchema flow.Schema

	// InternalStates contains declared internal state names.
	InternalStates []string

	// ExternalStates contains owned external state schemas.
	ExternalStates map[string]flow.Schema

	JoinedExternalStates map[string]flow.Schema

	// Parameters is the parameters map of the static spec, serialized as YSON.
	Parameters map[string]any

	DynamicParameters map[string]any
}

// Harness runs one computation and preserves state between batches.
type Harness struct {
	tb          testing.TB
	computation *flow.Computation
	job         *flow.Job

	keySchema flow.Schema
	streams   map[string]flow.Schema
	external  map[string]flow.Schema
	joined    map[string]flow.Schema

	watermarks map[string]uint64

	internalStates map[string]*stateStore[flow.InternalState]
	externalStates map[string]*stateStore[flow.ExternalState]
	joinedStates   map[string]*stateStore[flow.ExternalState]
}

// New returns a harness driving computation under the given configuration.
func New(tb testing.TB, computation *flow.Computation, opts Options) *Harness {
	tb.Helper()

	if computation == nil {
		tb.Fatalf("flowtest: computation is nil")
		return nil
	}

	h := &Harness{
		tb:             tb,
		computation:    computation,
		keySchema:      opts.KeySchema,
		streams:        maps.Clone(opts.Streams),
		external:       maps.Clone(opts.ExternalStates),
		joined:         maps.Clone(opts.JoinedExternalStates),
		watermarks:     map[string]uint64{},
		internalStates: map[string]*stateStore[flow.InternalState]{},
		externalStates: map[string]*stateStore[flow.ExternalState]{},
		joinedStates:   map[string]*stateStore[flow.ExternalState]{},
	}

	job, err := buildJob(computation.ID(), opts)
	if err != nil {
		tb.Fatalf("flowtest: %v", err)
		return nil
	}
	h.job = job

	return h
}

func buildJob(computationID string, opts Options) (*flow.Job, error) {
	streamIDs := make(map[string]int64, len(opts.Streams))
	streams := make([]flow.Stream, 0, len(opts.Streams))
	for specID, streamID := range slices.Sorted(maps.Keys(opts.Streams)) {
		streamIDs[streamID] = int64(specID)
		streams = append(streams, flow.NewStream(streamID, opts.Streams[streamID]))
	}

	parameters := maps.Clone(opts.Parameters)
	if parameters == nil {
		parameters = map[string]any{}
	}
	if len(opts.InternalStates) > 0 {
		parameters["internal_states"] = opts.InternalStates
	}

	staticSpec, err := yson.Marshal(map[string]any{
		"parameters":              parameters,
		"group_by_schema":         opts.KeySchema.Table(),
		"external_state_managers": stateSpecs(opts.ExternalStates),
		"external_state_joiners":  stateSpecs(opts.JoinedExternalStates),
	})
	if err != nil {
		return nil, xerrors.Errorf("rendering the static spec: %w", err)
	}

	dynamicParameters := opts.DynamicParameters
	if dynamicParameters == nil {
		dynamicParameters = map[string]any{}
	}
	dynamicSpec, err := yson.Marshal(map[string]any{"parameters": dynamicParameters})
	if err != nil {
		return nil, xerrors.Errorf("rendering the dynamic spec: %w", err)
	}

	return flow.NewJob(guid.New(), computationID, flow.NewStreamSpecs(streamIDs, streams), staticSpec, dynamicSpec)
}

func stateSpecs(states map[string]flow.Schema) map[string]any {
	specs := make(map[string]any, len(states))
	for name := range states {
		specs[name] = map[string]any{}
	}
	return specs
}

// Key builds a key of the configured key schema.
func (h *Harness) Key(fields Row) flow.Payload {
	h.tb.Helper()
	return h.payload(h.keySchema, fields, "key")
}

// Message builds an unkeyed message.
func (h *Harness) Message(streamID string, fields Row) flow.ExtendedMessage {
	h.tb.Helper()
	return h.KeyedMessage(streamID, flow.Payload{}, fields)
}

// KeyedMessage builds a keyed message.
func (h *Harness) KeyedMessage(streamID string, key flow.Payload, fields Row) flow.ExtendedMessage {
	h.tb.Helper()

	stream, ok := h.streams[streamID]
	if !ok {
		h.tb.Fatalf("flowtest: unknown stream %q, declared: %v", streamID, slices.Sorted(maps.Keys(h.streams)))
		return flow.ExtendedMessage{}
	}
	specID, _ := h.job.StreamSpecs().SpecID(streamID)

	return flow.ExtendedMessage{
		Message: flow.Message{
			Meta: flow.Meta{
				ID:           guid.New().String(),
				StreamID:     streamID,
				StreamSpecID: specID,
			},
			Payload: h.payload(stream, fields, fmt.Sprintf("message on stream %q", streamID)),
		},
		Key: key,
	}
}

// Timer builds a timer firing for a key.
func (h *Harness) Timer(key flow.Payload, triggerTimestamp uint64) flow.Timer {
	h.tb.Helper()
	return flow.Timer{
		Meta:             flow.Meta{ID: guid.New().String(), StreamSpecID: flow.NoStreamSpecID},
		TriggerTimestamp: triggerTimestamp,
		Key:              key,
	}
}

// Visit builds a visit of a key emitted by a key-visitor stream.
func (h *Harness) Visit(key flow.Payload) flow.Visit {
	h.tb.Helper()
	return flow.NewVisit(flow.Meta{ID: guid.New().String()}, key)
}

// SetWatermark records a stream watermark for subsequent runs.
func (h *Harness) SetWatermark(streamID string, watermark uint64) {
	h.tb.Helper()

	if _, ok := h.streams[streamID]; !ok {
		h.tb.Fatalf("flowtest: unknown stream %q, declared: %v", streamID, slices.Sorted(maps.Keys(h.streams)))
		return
	}
	h.watermarks[streamID] = watermark
}

// PutInternalState stores the bytes an internal state holds for a key before the run.
func (h *Harness) PutInternalState(name string, key flow.Payload, data []byte) {
	h.tb.Helper()

	if err := h.job.ValidateInternalStateName(name); err != nil {
		h.tb.Fatalf("flowtest: %v", err)
		return
	}
	storeFor(h.internalStates, name).put(h.stateKey(key), key, flow.InternalState{Data: data})
}

// PutInternalStateYSON stores an internal state as YSON.
func (h *Harness) PutInternalStateYSON(name string, key flow.Payload, value any) {
	h.tb.Helper()

	data, err := yson.MarshalFormat(value, yson.FormatBinary)
	if err != nil {
		h.tb.Fatalf("flowtest: internal state %q: %v", name, err)
		return
	}
	h.PutInternalState(name, key, data)
}

// PutInternalStateProto stores an internal state as protobuf.
func (h *Harness) PutInternalStateProto(name string, key flow.Payload, value proto.Message) {
	h.tb.Helper()

	data, err := proto.Marshal(value)
	if err != nil {
		h.tb.Fatalf("flowtest: internal state %q: %v", name, err)
		return
	}
	h.PutInternalState(name, key, data)
}

// PutExternalState stores an owned external state row.
func (h *Harness) PutExternalState(name string, key flow.Payload, fields Row) {
	h.tb.Helper()

	stateSchema, ok := h.external[name]
	if !ok {
		h.tb.Fatalf("flowtest: unknown external state %q, declared: %v", name, slices.Sorted(maps.Keys(h.external)))
		return
	}
	value := flow.ExternalState{Value: h.payload(stateSchema, fields, fmt.Sprintf("external state %q", name))}
	storeFor(h.externalStates, name).put(h.stateKey(key), key, value)
}

// PutJoinedExternalState stores the row a joined external state holds for a key.
func (h *Harness) PutJoinedExternalState(name string, key flow.Payload, fields Row) {
	h.tb.Helper()

	stateSchema, ok := h.joined[name]
	if !ok {
		h.tb.Fatalf("flowtest: unknown joined external state %q, declared: %v",
			name, slices.Sorted(maps.Keys(h.joined)))
		return
	}
	value := flow.ExternalState{Value: h.payload(stateSchema, fields, fmt.Sprintf("joined external state %q", name))}
	storeFor(h.joinedStates, name).put(h.stateKey(key), key, value)
}

// Process runs one batch and fails the test on error.
func (h *Harness) Process(inputs ...flow.Input) *Response {
	h.tb.Helper()

	response, err := h.process(inputs)
	if err != nil {
		h.tb.Fatalf("flowtest: %v", err)
		return nil
	}
	return response
}

// ProcessError runs one batch and returns its expected error.
func (h *Harness) ProcessError(inputs ...flow.Input) error {
	h.tb.Helper()

	if _, err := h.process(inputs); err != nil {
		return err
	}
	h.tb.Fatalf("flowtest: computation %q processed the batch without an error", h.computation.ID())
	return nil
}

func (h *Harness) process(inputs []flow.Input) (*Response, error) {
	batch := h.batch(inputs)

	runtime := flow.NewRequestRuntime(h.job)
	for _, streamID := range slices.Sorted(maps.Keys(h.watermarks)) {
		runtime.SetWatermark(streamID, h.watermarks[streamID])
	}
	if err := h.loadStates(runtime, batch); err != nil {
		return nil, err
	}

	results, err := h.computation.Process(context.Background(), runtime, batch)
	if err != nil {
		return nil, err
	}

	// The wire form is rendered and dropped: it is where a message on an undeclared stream,
	// an unencodable key and a state written as empty bytes are refused.
	if _, err := flow.ResponseDataToProto(runtime, results); err != nil {
		return nil, err
	}

	h.applyStates(runtime)

	return &Response{tb: h.tb, results: results, runtime: runtime}, nil
}

func (h *Harness) batch(inputs []flow.Input) flow.Batch {
	var batch flow.Batch
	for i, input := range inputs {
		switch input := input.(type) {
		case flow.ExtendedMessage:
			batch.Messages = append(batch.Messages, input)
		case flow.Timer:
			batch.Timers = append(batch.Timers, input)
		case flow.Visit:
			batch.Visits = append(batch.Visits, input)
		default:
			h.tb.Fatalf("flowtest: input %d is a %T, want a message, a timer or a visit", i, input)
			return flow.Batch{}
		}
	}
	return batch
}

func (h *Harness) loadStates(runtime *flow.RequestRuntime, batch flow.Batch) error {
	keys := h.batchKeys(batch)

	for _, name := range slices.Sorted(maps.Keys(h.internalStates)) {
		stored := h.internalStates[name]
		for _, key := range keys {
			value, ok := stored.get(h.stateKey(key))
			if !ok {
				continue
			}
			if err := runtime.LoadInternalState(name, key, value); err != nil {
				return err
			}
		}
	}

	for _, name := range slices.Sorted(maps.Keys(h.external)) {
		stateSchema := h.external[name]
		stored := storeFor(h.externalStates, name)
		for _, key := range keys {
			value, ok := stored.get(h.stateKey(key))
			if !ok {
				value = flow.ExternalState{Value: flow.NewPayload(nil, stateSchema)}
			}
			if err := runtime.LoadExternalState(name, stateSchema, key, value); err != nil {
				return err
			}
		}
	}

	for _, name := range slices.Sorted(maps.Keys(h.joined)) {
		stateSchema := h.joined[name]
		stored, ok := h.joinedStates[name]
		if !ok {
			continue
		}
		for _, key := range keys {
			value, ok := stored.get(h.stateKey(key))
			if !ok {
				continue
			}
			if err := runtime.LoadJoinedExternalState(name, stateSchema, key, value); err != nil {
				return err
			}
		}
	}

	return nil
}

func (h *Harness) applyStates(runtime *flow.RequestRuntime) {
	for holder := range runtime.ModifiedInternalStates() {
		applyModified(h, h.internalStates, holder, func(value flow.InternalState) bool { return value.Reset })
	}

	for holder := range runtime.ModifiedExternalStates() {
		applyModified(h, h.externalStates, holder, func(value flow.ExternalState) bool { return value.Reset })
	}
}

func applyModified[T flow.StateValue[T]](
	h *Harness,
	stores map[string]*stateStore[T],
	holder *flow.StatesHolder[T],
	reset func(T) bool,
) {
	stored := storeFor(stores, holder.Name())
	for key, value := range holder.Modified() {
		encoded := h.stateKey(key)
		if reset(value) {
			stored.delete(encoded)
			continue
		}
		stored.put(encoded, key, value)
	}
}

func (h *Harness) batchKeys(batch flow.Batch) []flow.Payload {
	var keys []flow.Payload
	seen := map[string]bool{}

	add := func(key flow.Payload) {
		encoded := h.stateKey(key)
		if seen[encoded] {
			return
		}
		seen[encoded] = true
		keys = append(keys, key)
	}

	for _, msg := range batch.Messages {
		add(msg.Key)
	}
	for _, timer := range batch.Timers {
		add(timer.Key)
	}
	for _, visit := range batch.Visits {
		add(visit.Key)
	}
	return keys
}

func (h *Harness) payload(s flow.Schema, fields Row, what string) flow.Payload {
	h.tb.Helper()

	b := flow.NewPayloadBuilder(s)
	for _, column := range slices.Sorted(maps.Keys(fields)) {
		b.Set(column, fields[column])
	}

	p, err := b.Finish()
	if err != nil {
		h.tb.Fatalf("flowtest: %s: %v", what, err)
		return flow.Payload{}
	}
	return p
}

func (h *Harness) stateKey(key flow.Payload) string {
	encoded, err := wire.MarshalRowProto(key.Row())
	if err != nil {
		h.tb.Fatalf("flowtest: unusable key: %v", err)
		return ""
	}
	return string(encoded)
}

func storeFor[T any](stores map[string]*stateStore[T], name string) *stateStore[T] {
	store, ok := stores[name]
	if !ok {
		store = &stateStore[T]{index: map[string]int{}}
		stores[name] = store
	}
	return store
}

type stateStore[T any] struct {
	index   map[string]int
	entries []stateEntry[T]
}

type stateEntry[T any] struct {
	encoded string
	key     flow.Payload
	value   T
}

func (s *stateStore[T]) get(encoded string) (T, bool) {
	var zero T
	i, ok := s.index[encoded]
	if !ok {
		return zero, false
	}
	return s.entries[i].value, true
}

func (s *stateStore[T]) put(encoded string, key flow.Payload, value T) {
	entry := stateEntry[T]{encoded: encoded, key: key, value: value}
	if i, ok := s.index[encoded]; ok {
		s.entries[i] = entry
		return
	}
	s.index[encoded] = len(s.entries)
	s.entries = append(s.entries, entry)
}

func (s *stateStore[T]) delete(encoded string) {
	i, ok := s.index[encoded]
	if !ok {
		return
	}
	s.entries = slices.Delete(s.entries, i, i+1)

	s.index = make(map[string]int, len(s.entries))
	for i := range s.entries {
		s.index[s.entries[i].encoded] = i
	}
}

// Schema builds a table schema from name:type columns.
func Schema(columns ...string) flow.Schema {
	table := schema.Schema{Columns: make([]schema.Column, 0, len(columns))}

	for _, column := range columns {
		name, typeName, ok := strings.Cut(column, ":")
		if !ok {
			panic(fmt.Sprintf("flowtest: column %q is not spelled name:type", column))
		}
		columnType, err := parseType(typeName)
		if err != nil {
			panic(fmt.Sprintf("flowtest: column %q: %v", column, err))
		}
		table.Columns = append(table.Columns, schema.Column{Name: name, Type: columnType})
	}

	return flow.NewSchema(table)
}

// SchemaOf infers a table schema from a struct.
func SchemaOf(v any) flow.Schema {
	table, err := schema.Infer(v)
	if err != nil {
		panic(fmt.Sprintf("flowtest: %v", err))
	}
	return flow.NewSchema(table)
}

func parseType(name string) (schema.Type, error) {
	encoded, err := yson.Marshal(name)
	if err != nil {
		return "", err
	}

	var parsed schema.ComplexType
	if err := yson.Unmarshal(encoded, &parsed); err != nil {
		return "", err
	}

	columnType, ok := parsed.(schema.Type)
	if !ok {
		return "", xerrors.Errorf("%q is not a primitive type", name)
	}

	_, ok = flow.NewSchema(schema.Schema{Columns: []schema.Column{{Type: columnType}}}).ColumnType(0)
	if !ok {
		return "", xerrors.Errorf("unknown type %q", name)
	}
	return columnType, nil
}

// ToRow decodes the non-null cells of a payload.
func ToRow(p flow.Payload) Row {
	fields := Row{}

	for _, column := range p.Columns() {
		value, ok := p.Value(column)
		if !ok {
			continue
		}
		switch value.Type {
		case wire.TypeInt64:
			fields[column] = value.Int64()
		case wire.TypeUint64:
			fields[column] = value.Uint64()
		case wire.TypeFloat64:
			fields[column] = value.Float64()
		case wire.TypeBool:
			fields[column] = value.Bool()
		case wire.TypeBytes:
			fields[column] = string(value.Bytes())
		case wire.TypeAny, wire.TypeComposite:
			fields[column] = value.Bytes()
		}
	}

	return fields
}
