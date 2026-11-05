package flow

import (
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/schema"
)

// Meta is the envelope every input carries.
type Meta struct {
	// ID is assigned by the worker. Locally built outputs leave it empty.
	ID              string
	EventTimestamp  uint64
	SystemTimestamp uint64
	StreamID        string
	// StreamSpecID identifies an input stream schema revision. Outputs ignore it.
	StreamSpecID int64
}

// Message is an input or output message of a computation.
type Message struct {
	Meta
	Payload Payload
}

// ExtendedMessage is a Message delivered to a keyed computation, carrying the grouping
// key it was partitioned by.
type ExtendedMessage struct {
	Message
	Key Payload
}

func (m ExtendedMessage) PartitionKey() Payload {
	return m.Key
}

// Timer is a timer firing for a key.
type Timer struct {
	Meta
	TriggerTimestamp uint64
	Key              Payload
}

func (t Timer) PartitionKey() Payload {
	return t.Key
}

// Visit is a visit of a key emitted by a key-visitor stream.
type Visit struct {
	Meta
	Key Payload
}

// NewVisit builds a Visit.
func NewVisit(meta Meta, key Payload) Visit {
	meta.StreamSpecID = NoStreamSpecID
	return Visit{Meta: meta, Key: key}
}

func (v Visit) PartitionKey() Payload {
	return v.Key
}

// Input is a message, timer, or visit delivered to a computation.
type Input interface {
	PartitionKey() Payload
}

var (
	_ Input = ExtendedMessage{}
	_ Input = Timer{}
	_ Input = Visit{}
)

// TimerRequest is a timer a computation asks the worker to set.
type TimerRequest struct {
	TriggerTimestamp uint64
	EventTimestamp   uint64
	// StreamID selects the timer stream. Empty means the pipeline's only timer stream.
	StreamID string
}

// MessageBuilder builds messages for one output stream.
type MessageBuilder struct {
	streamID string
	payload  *PayloadBuilder

	eventTimestamp  uint64
	systemTimestamp uint64
}

func newMessageBuilder(streamID string, s Schema) *MessageBuilder {
	return &MessageBuilder{streamID: streamID, payload: NewPayloadBuilder(s)}
}

// StreamID returns the stream the built messages belong to.
func (b *MessageBuilder) StreamID() string {
	return b.streamID
}

// Schema returns the schema of the message payload.
func (b *MessageBuilder) Schema() Schema {
	return b.payload.Schema()
}

// Set stores value in the named payload column.
func (b *MessageBuilder) Set(column string, value any) *MessageBuilder {
	b.payload.Set(column, value)
	return b
}

// SetStruct stores every payload column the yson tags of the struct v name.
func (b *MessageBuilder) SetStruct(v any) *MessageBuilder {
	b.payload.SetStruct(v)
	return b
}

// SetEventTimestamp sets the event timestamp.
func (b *MessageBuilder) SetEventTimestamp(ts uint64) *MessageBuilder {
	b.eventTimestamp = ts
	return b
}

// SetSystemTimestamp sets the message creation timestamp.
func (b *MessageBuilder) SetSystemTimestamp(ts uint64) *MessageBuilder {
	b.systemTimestamp = ts
	return b
}

// Finish returns the message.
func (b *MessageBuilder) Finish() (Message, error) {
	meta := Meta{
		EventTimestamp:  b.eventTimestamp,
		SystemTimestamp: b.systemTimestamp,
		StreamID:        b.streamID,
	}
	payload, err := b.payload.Finish()
	if err != nil {
		return Message{}, err
	}
	return Message{Meta: meta, Payload: payload}, nil
}

// YSONMessage is embedded in typed message structs.
type YSONMessage struct {
	Meta Meta `yson:"-"`
}

func (m *YSONMessage) ysonMessageMeta() *Meta {
	return &m.Meta
}

// YSONMessageValue is a message struct that embeds YSONMessage.
type YSONMessageValue interface {
	ysonMessageMeta() *Meta
}

type ysonMessagePointer[T any] interface {
	*T
	YSONMessageValue
}

// NewYSONMessage creates a typed output message for streamID.
func NewYSONMessage[T any, PT ysonMessagePointer[T]](streamID string) PT {
	message := PT(new(T))
	message.ysonMessageMeta().StreamID = streamID
	return message
}

// ConvertTo decodes a message into a typed YSON value.
func (m Message) ConvertTo(value YSONMessageValue) error {
	if err := m.Payload.ConvertTo(value); err != nil {
		return xerrors.Errorf("flow: convert message: %w", err)
	}
	*value.ysonMessageMeta() = m.Meta
	return nil
}

// ConvertFrom encodes a typed YSON value as a message.
func ConvertFrom(rt Runtime, value YSONMessageValue) (Message, error) {
	meta := *value.ysonMessageMeta()
	builder, err := rt.MessageBuilder(meta.StreamID)
	if err != nil {
		return Message{}, err
	}
	builder.SetEventTimestamp(meta.EventTimestamp)
	builder.SetSystemTimestamp(meta.SystemTimestamp)
	message, err := builder.SetStruct(value).Finish()
	if err != nil {
		return Message{}, xerrors.Errorf("flow: convert message: %w", err)
	}
	return message, nil
}

// YSONMessageSchema infers a stream schema from a typed message.
func YSONMessageSchema[T any, PT ysonMessagePointer[T]]() Schema {
	table := schema.MustInfer(PT(new(T)))
	for i := range table.Columns {
		table.Columns[i].Required = false
		if table.Columns[i].Type == schema.TypeString {
			table.Columns[i].Type = schema.TypeBytes
		}
	}
	return NewSchema(table)
}

// NewYSONStream declares a stream whose schema is inferred from a typed message.
func NewYSONStream[T any, PT ysonMessagePointer[T]](id string) Stream {
	return NewStream(id, YSONMessageSchema[T, PT]())
}
