package bus

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/compression"
	"go.ytsaurus.tech/yt/go/proto/core/rpc"
	"go.ytsaurus.tech/yt/go/yterrors"
)

// NOTE: See yt/yt/core/rpc/stream.cpp for the reference implementation of the protocol.

const (
	// DefaultStreamingWindowSize is the default limit for the number of attachment bytes in flight.
	//
	// It is used both for the client-to-server window (local to the client)
	// and for the server-to-client window (sent to the server in the request header).
	DefaultStreamingWindowSize = 16 * 1024 * 1024

	// maxStreamingPayloadGap limits how far ahead of the expected sequence number
	// an incoming payload may be. Similar to MaxWindowSize in yt/yt/core/rpc/stream.cpp.
	maxStreamingPayloadGap = 16384
)

// streamingAttachmentSize returns the size of the attachment for the purposes of flow control.
//
// Empty and null attachments count as 1 byte, as in GetStreamingAttachmentSize in yt/yt/core/rpc/stream.cpp.
func streamingAttachmentSize(attachment []byte) int64 {
	if len(attachment) == 0 {
		return 1
	}
	return int64(len(attachment))
}

func buildStreamingPayloadMsg(reqHeader *rpc.TRequestHeader, seq int32, codec int32, attachments [][]byte) ([][]byte, error) {
	header := &rpc.TStreamingPayloadHeader{
		RequestId:      reqHeader.RequestId,
		Service:        reqHeader.Service,
		Method:         reqHeader.Method,
		RealmId:        reqHeader.RealmId,
		SequenceNumber: ptr.Int32(seq),
		Codec:          ptr.Int32(codec),
	}

	first, err := marshalMsgHeader(msgStreamPayload, header)
	if err != nil {
		return nil, err
	}

	msg := make([][]byte, 1+len(attachments))
	msg[0] = first
	copy(msg[1:], attachments)
	return msg, nil
}

func buildStreamingFeedbackMsg(reqHeader *rpc.TRequestHeader, readPosition int64) ([][]byte, error) {
	header := &rpc.TStreamingFeedbackHeader{
		RequestId:    reqHeader.RequestId,
		Service:      reqHeader.Service,
		Method:       reqHeader.Method,
		RealmId:      reqHeader.RealmId,
		ReadPosition: ptr.Int64(readPosition),
	}

	first, err := marshalMsgHeader(msgStreamFeedback, header)
	if err != nil {
		return nil, err
	}

	return [][]byte{first}, nil
}

func marshalMsgHeader(typ msgType, header proto.Message) ([]byte, error) {
	data, err := proto.Marshal(header)
	if err != nil {
		return nil, err
	}

	res := make([]byte, 4, 4+len(data))
	binary.LittleEndian.PutUint32(res, uint32(typ))
	return append(res, data...), nil
}

func parseStreamingPayloadMsg(msg [][]byte) (*rpc.TStreamingPayloadHeader, [][]byte, error) {
	var header rpc.TStreamingPayloadHeader
	if err := proto.Unmarshal(msg[0][4:], &header); err != nil {
		return nil, nil, fmt.Errorf("bus: error parsing streaming payload header: %w", err)
	}
	if header.RequestId == nil {
		return nil, nil, fmt.Errorf("bus: streaming payload is missing request_id")
	}
	if header.SequenceNumber == nil {
		return nil, nil, fmt.Errorf("bus: streaming payload is missing sequence_number")
	}
	return &header, msg[1:], nil
}

func parseStreamingFeedbackMsg(msg [][]byte) (*rpc.TStreamingFeedbackHeader, error) {
	var header rpc.TStreamingFeedbackHeader
	if err := proto.Unmarshal(msg[0][4:], &header); err != nil {
		return nil, fmt.Errorf("bus: error parsing streaming feedback header: %w", err)
	}
	if header.RequestId == nil {
		return nil, fmt.Errorf("bus: streaming feedback is missing request_id")
	}
	if header.ReadPosition == nil {
		return nil, fmt.Errorf("bus: streaming feedback is missing read_position")
	}
	return &header, nil
}

// streamMsg is a streaming payload or feedback message queued for sending.
type streamMsg struct {
	stream *ClientStream
	// msg is a prebuilt payload message. When nil, a feedback message
	// with the latest read position of the stream is built at send time.
	msg [][]byte
}

// ClientStream is a streaming RPC call.
//
// Request attachments are written with Write and CloseSend,
// response attachments are read with Read, and the final response is received with Wait.
type ClientStream struct {
	conn *ClientConn
	req  *clientReq
	opts []SendOption

	mu   sync.Mutex
	cond *sync.Cond

	// Client-to-server stream.
	windowSize       int64
	writePosition    int64
	peerReadPosition int64
	nextSendSeq      int32
	sendClosed       bool
	sendErr          error

	// Server-to-client stream.
	pending         map[int32][][]byte
	nextRecvSeq     int32
	ready           [][]byte
	eosReceived     bool
	eosRead         bool
	readPosition    int64
	feedbackQueued  bool
	recvErr         error
	responseDone    bool
	responseErr     error
	aborted         bool
	abortErr        error
	afterOptsCalled bool

	finishOnce sync.Once
	finished   chan struct{}
}

func newClientStream(conn *ClientConn, req *clientReq, opts []SendOption) *ClientStream {
	s := &ClientStream{
		conn:       conn,
		req:        req,
		opts:       opts,
		windowSize: conn.streamingWindowSize,
		pending:    make(map[int32][][]byte),
		finished:   make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// SendStreaming starts a streaming call.
//
// The call is sent before SendStreaming returns. Context cancellation or deadline
// aborts the call at any moment of its lifetime.
//
// The request timeout is never sent for a streaming call. Server-to-client window size
// and stall timeouts are sent in server_attachments_streaming_parameters, see WithStreamingTimeouts.
func (c *ClientConn) SendStreaming(
	ctx context.Context,
	service, method string,
	request, reply proto.Message,
	opts ...SendOption,
) (*ClientStream, error) {
	req := c.newClientReq(service, method, request, reply)
	req.sent = make(chan struct{})

	for _, opt := range opts {
		opt.before(req)
	}

	// Streaming calls are limited by the context and stall timeouts, not by the request timeout.
	req.reqHeader.Timeout = nil

	if req.reqHeader.ServerAttachmentsStreamingParameters == nil {
		req.reqHeader.ServerAttachmentsStreamingParameters = &rpc.TStreamingParameters{}
	}
	if req.reqHeader.ServerAttachmentsStreamingParameters.WindowSize == nil {
		req.reqHeader.ServerAttachmentsStreamingParameters.WindowSize = ptr.Int64(DefaultStreamingWindowSize)
	}

	s := newClientStream(c, req, opts)
	req.stream = s

	select {
	case c.sendQueue <- req:
	case <-c.stop:
		return nil, c.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// NB: Wait for the request to be sent, so that no payload or feedback is sent before it.
	select {
	case <-req.sent:
	case err := <-req.done:
		return nil, err
	case <-c.stop:
		return nil, c.Err()
	}

	go s.watch(ctx, req.acknowledgementTimeout)

	return s, nil
}

func (s *ClientStream) watch(ctx context.Context, ackTimeout *time.Duration) {
	var ackTimer <-chan time.Time
	if ackTimeout != nil {
		t := time.NewTimer(*ackTimeout)
		defer t.Stop()
		ackTimer = t.C
	}

	for {
		select {
		case err := <-s.req.done:
			s.handleResponse(err)

		case <-ackTimer:
			ackTimer = nil
			if !s.req.acked.Load() && !s.isResponseDone() {
				err := yterrors.Err(yterrors.CodeTimeout, "Request acknowledgement timed out")
				s.conn.log.Error("Request acknowledgement timeout", log.Error(err))
				s.Abort(err)
			}

		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				s.Abort(yterrors.Err(yterrors.CodeTimeout, "Request timed out"))
			} else {
				s.Abort(yterrors.Err(yterrors.CodeCanceled, "Request canceled"))
			}

		case <-s.conn.stop:
			s.Abort(s.conn.Err())

		case <-s.finished:
			return
		}
	}
}

func (s *ClientStream) isResponseDone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.responseDone
}

// handleResponse is called once the final response (or a send error) is received.
func (s *ClientStream) handleResponse(err error) {
	s.mu.Lock()
	if s.responseDone || s.aborted {
		s.mu.Unlock()
		return
	}

	s.responseDone = true
	if err != nil {
		s.conn.enrichClientRequestErrorWithFeatureName(err)
		s.responseErr = err
		s.failSendLocked(err)
		s.failRecvLocked(err)
	} else if !s.sendClosed {
		// There is no chance the server will receive any of newly written data.
		s.failSendLocked(yterrors.Err("Request is already completed"))
	}

	finished := err != nil || s.eosReceived
	s.cond.Broadcast()
	s.mu.Unlock()

	if finished {
		s.finish()
	}
}

func (s *ClientStream) failSendLocked(err error) {
	if s.sendErr == nil {
		s.sendErr = err
	}
}

func (s *ClientStream) failRecvLocked(err error) {
	if s.recvErr == nil {
		s.recvErr = err
	}
}

// finish unregisters the request from the connection.
func (s *ClientStream) finish() {
	s.finishOnce.Do(func() {
		close(s.finished)

		s.conn.l.Lock()
		delete(s.conn.reqs, s.req.id)
		s.conn.l.Unlock()

		s.conn.deleteUnackedReq(s.req.id)
	})
}

// handlePayload is called by the receiver for every streaming payload of this call.
func (s *ClientStream) handlePayload(header *rpc.TStreamingPayloadHeader, attachments [][]byte) {
	codecID := compression.CodecIDNone
	if header.Codec != nil {
		codecID = compression.CodecID(*header.Codec)
	}

	if codecID != compression.CodecIDNone {
		codec := compression.NewCodec(codecID)
		decompressed := make([][]byte, len(attachments))
		for i, a := range attachments {
			if a == nil {
				continue
			}

			d, err := codec.Decompress(a)
			if err != nil {
				s.Abort(fmt.Errorf("bus: error decompressing streaming payload: %w", err))
				return
			}
			if d == nil {
				d = []byte{}
			}
			decompressed[i] = d
		}
		attachments = decompressed
	}

	seq := header.GetSequenceNumber()

	s.mu.Lock()
	if s.recvErr != nil || s.eosReceived {
		s.mu.Unlock()
		return
	}

	var protocolErr error
	switch _, ok := s.pending[seq]; {
	case seq < s.nextRecvSeq || ok:
		protocolErr = yterrors.Err(yterrors.CodeProtocolError, "Duplicate streaming payload",
			yterrors.Attr("sequence_number", seq))
	case seq >= s.nextRecvSeq+maxStreamingPayloadGap:
		protocolErr = yterrors.Err(yterrors.CodeProtocolError, "Streaming payload sequence number is too large",
			yterrors.Attr("sequence_number", seq),
			yterrors.Attr("expected_sequence_number", s.nextRecvSeq))
	}

	if protocolErr != nil {
		s.mu.Unlock()
		s.Abort(protocolErr)
		return
	}

	s.pending[seq] = attachments
	for {
		next, ok := s.pending[s.nextRecvSeq]
		if !ok {
			break
		}
		delete(s.pending, s.nextRecvSeq)
		s.nextRecvSeq++

		for _, a := range next {
			s.ready = append(s.ready, a)
			if a == nil {
				s.eosReceived = true
				break
			}
		}

		if s.eosReceived {
			s.pending = nil
			break
		}
	}

	finished := s.eosReceived && s.responseDone
	s.cond.Broadcast()
	s.mu.Unlock()

	if finished {
		s.finish()
	}
}

// handleFeedback is called by the receiver for every streaming feedback of this call.
func (s *ClientStream) handleFeedback(header *rpc.TStreamingFeedbackHeader) {
	readPosition := header.GetReadPosition()

	s.mu.Lock()
	if readPosition > s.writePosition {
		writePosition := s.writePosition
		s.mu.Unlock()
		s.Abort(yterrors.Err(yterrors.CodeProtocolError, "Read position in streaming feedback is beyond write position",
			yterrors.Attr("read_position", readPosition),
			yterrors.Attr("write_position", writePosition)))
		return
	}

	if readPosition > s.peerReadPosition {
		s.peerReadPosition = readPosition
		s.cond.Broadcast()
	}
	s.mu.Unlock()
}

// buildFeedback builds a feedback message with the latest read position.
func (s *ClientStream) buildFeedback() ([][]byte, error) {
	s.mu.Lock()
	readPosition := s.readPosition
	s.feedbackQueued = false
	s.mu.Unlock()

	return buildStreamingFeedbackMsg(s.req.reqHeader, readPosition)
}

// Write sends an attachment to the server.
//
// Write blocks while the attachment does not fit into the client-to-server window.
// A single attachment larger than the window is sent when nothing is in flight.
//
// Data is sent asynchronously and must not be modified after the call.
func (s *ClientStream) Write(data []byte) error {
	if data == nil {
		data = []byte{}
	}
	return s.send(data)
}

// CloseSend sends the end of the client-to-server stream.
func (s *ClientStream) CloseSend() error {
	return s.send(nil)
}

func (s *ClientStream) send(attachment []byte) error {
	size := streamingAttachmentSize(attachment)

	s.mu.Lock()
	for {
		if s.sendErr != nil {
			err := s.sendErr
			s.mu.Unlock()
			return err
		}
		if s.sendClosed {
			s.mu.Unlock()
			if attachment == nil {
				return nil
			}
			return yterrors.Err("Stream is already closed")
		}

		inFlight := s.writePosition - s.peerReadPosition
		if inFlight+size <= s.windowSize || inFlight == 0 {
			break
		}
		s.cond.Wait()
	}

	seq := s.nextSendSeq
	s.nextSendSeq++
	s.writePosition += size
	if attachment == nil {
		s.sendClosed = true
	}
	s.mu.Unlock()

	msg, err := buildStreamingPayloadMsg(s.req.reqHeader, seq, int32(compression.CodecIDNone), [][]byte{attachment})
	if err != nil {
		s.Abort(err)
		return err
	}

	// NB: Do not hold the lock here, the sender takes it to build feedback.
	if err := s.conn.enqueueStreamMsg(streamMsg{stream: s, msg: msg}); err != nil {
		s.Abort(err)
		return err
	}

	return nil
}

// Read returns the next attachment sent by the server.
//
// Read returns io.EOF at the end of the server-to-client stream.
func (s *ClientStream) Read() ([]byte, error) {
	s.mu.Lock()
	for {
		if s.eosRead {
			s.mu.Unlock()
			return nil, io.EOF
		}
		if s.recvErr != nil {
			err := s.recvErr
			s.mu.Unlock()
			return nil, err
		}
		if len(s.ready) > 0 {
			break
		}
		s.cond.Wait()
	}

	attachment := s.ready[0]
	s.ready[0] = nil
	s.ready = s.ready[1:]
	s.readPosition += streamingAttachmentSize(attachment)
	if attachment == nil {
		s.eosRead = true
	}

	queueFeedback := !s.feedbackQueued
	s.feedbackQueued = true
	s.mu.Unlock()

	if queueFeedback {
		if err := s.conn.enqueueStreamMsg(streamMsg{stream: s}); err != nil {
			s.Abort(err)
			return nil, err
		}
	}

	if attachment == nil {
		return nil, io.EOF
	}
	return attachment, nil
}

// Wait blocks until the final response is received and returns its error.
//
// On success the response body is unmarshalled into the reply passed to SendStreaming.
func (s *ClientStream) Wait() error {
	s.mu.Lock()
	for !s.responseDone && !s.aborted {
		s.cond.Wait()
	}

	if !s.responseDone {
		err := s.abortErr
		s.mu.Unlock()
		return err
	}

	err := s.responseErr
	callAfter := err == nil && !s.afterOptsCalled
	s.afterOptsCalled = true
	s.mu.Unlock()

	if callAfter {
		for _, opt := range s.opts {
			opt.after(s.req)
		}
	}

	return err
}

// Abort cancels the call unless the final response is already received
// and fails all pending and later reads, writes and waits with the given error.
func (s *ClientStream) Abort(err error) {
	if err == nil {
		err = yterrors.Err(yterrors.CodeCanceled, "Stream aborted")
	}

	s.mu.Lock()
	select {
	case <-s.finished:
		s.mu.Unlock()
		return
	default:
	}

	cancel := !s.responseDone
	if !s.aborted && cancel {
		s.aborted = true
		s.abortErr = err
	}
	s.failSendLocked(err)
	s.failRecvLocked(err)
	s.cond.Broadcast()
	s.mu.Unlock()

	if cancel {
		s.conn.finishReq(s.req)
	}
	s.finish()
}
