package flow

// Stream is a named channel of messages with one schema.
type Stream struct {
	ID     string
	Schema Schema
}

// NewStream declares a stream.
func NewStream(id string, s Schema) Stream {
	return Stream{ID: id, Schema: s}
}

// StreamSpecs resolves streams by stream id and wire spec id.
type StreamSpecs struct {
	specIDByStreamID map[string]int64
	streamIDBySpecID map[int64]string
	streamByStreamID map[string]Stream
}

// NewStreamSpecs binds streams to spec ids.
func NewStreamSpecs(specIDs map[string]int64, streams []Stream) StreamSpecs {
	specs := StreamSpecs{
		specIDByStreamID: map[string]int64{},
		streamIDBySpecID: map[int64]string{},
		streamByStreamID: make(map[string]Stream, len(streams)),
	}
	for streamID, specID := range specIDs {
		specs.specIDByStreamID[streamID] = specID
		specs.streamIDBySpecID[specID] = streamID
	}
	for _, stream := range streams {
		if _, ok := specs.streamByStreamID[stream.ID]; !ok {
			specs.streamByStreamID[stream.ID] = stream
		}
	}
	return specs
}

// Stream returns the stream named streamID.
func (s StreamSpecs) Stream(streamID string) (Stream, bool) {
	stream, ok := s.streamByStreamID[streamID]
	return stream, ok
}

// StreamBySpecID returns the stream addressed by specID.
func (s StreamSpecs) StreamBySpecID(specID int64) (Stream, bool) {
	streamID, ok := s.streamIDBySpecID[specID]
	if !ok {
		return Stream{}, false
	}
	return s.Stream(streamID)
}

// StreamID returns the stream id addressed by specID.
func (s StreamSpecs) StreamID(specID int64) (string, bool) {
	streamID, ok := s.streamIDBySpecID[specID]
	return streamID, ok
}

// SpecID returns the spec id streamID is addressed by.
func (s StreamSpecs) SpecID(streamID string) (int64, bool) {
	specID, ok := s.specIDByStreamID[streamID]
	return specID, ok
}

// Len returns the number of streams carrying a schema.
func (s StreamSpecs) Len() int {
	return len(s.streamByStreamID)
}
