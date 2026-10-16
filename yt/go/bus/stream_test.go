package bus

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/compression"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/proto/core/rpc"
	testservice "go.ytsaurus.tech/yt/go/proto/core/rpc/unittests"
	"go.ytsaurus.tech/yt/go/yterrors"
)

// fakeServer is a minimal in-process RPC server speaking the bus protocol.
type fakeServer struct {
	t   *testing.T
	bus *Bus
}

type fakeMsg struct {
	typ   msgType
	parts [][]byte
}

// newFakeServerConn starts a fake server and returns a client connected to it.
func newFakeServerConn(t *testing.T, opts ...ClientOption) (*fakeServer, *ClientConn) {
	t.Helper()

	lsn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lsn.Close() })

	accepted := make(chan *Bus, 1)
	go func() {
		conn, err := lsn.Accept()
		if err != nil {
			close(accepted)
			return
		}

		b := NewBus(conn, Options{})
		if err := b.establishEncryption(true); err != nil {
			b.Close()
			close(accepted)
			return
		}
		accepted <- b
	}()

	client := NewClient(context.Background(), lsn.Addr().String(), opts...)
	t.Cleanup(client.Close)

	b, ok := <-accepted
	require.True(t, ok, "fake server failed to accept connection")
	t.Cleanup(b.Close)

	return &fakeServer{t: t, bus: b}, client
}

// recv receives the next message packet, acknowledging it when requested.
func (s *fakeServer) recv() fakeMsg {
	s.t.Helper()

	for {
		msg, err := s.bus.Receive()
		require.NoError(s.t, err)

		if msg.fixHeader.typ != packetMessage {
			continue
		}

		if msg.fixHeader.flags&packetFlagsRequestAcknowledgement != 0 {
			ack := busMsg{fixHeader: fixedHeader{
				signature: packetSignature,
				typ:       packetAck,
				packetID:  msg.fixHeader.packetID,
			}}
			_, err := ack.writeTo(s.bus.conn)
			require.NoError(s.t, err)
		}

		return fakeMsg{
			typ:   msgType(binary.LittleEndian.Uint32(msg.parts[0][:4])),
			parts: msg.parts,
		}
	}
}

func (s *fakeServer) recvRequest() *rpc.TRequestHeader {
	s.t.Helper()

	msg := s.recv()
	require.Equal(s.t, msgRequest, msg.typ)

	var header rpc.TRequestHeader
	require.NoError(s.t, proto.Unmarshal(msg.parts[0][4:], &header))
	return &header
}

func (s *fakeServer) recvPayload() (*rpc.TStreamingPayloadHeader, [][]byte) {
	s.t.Helper()

	msg := s.recv()
	require.Equal(s.t, msgStreamPayload, msg.typ)

	header, attachments, err := parseStreamingPayloadMsg(msg.parts)
	require.NoError(s.t, err)
	return header, attachments
}

func (s *fakeServer) recvFeedback() int64 {
	s.t.Helper()

	msg := s.recv()
	require.Equal(s.t, msgStreamFeedback, msg.typ)

	header, err := parseStreamingFeedbackMsg(msg.parts)
	require.NoError(s.t, err)
	return header.GetReadPosition()
}

func (s *fakeServer) send(msg [][]byte) {
	s.t.Helper()
	require.NoError(s.t, s.bus.Send(guid.New(), msg, &busSendOptions{}))
}

func (s *fakeServer) sendPayload(req *rpc.TRequestHeader, seq int32, attachments ...[]byte) {
	s.t.Helper()

	msg, err := buildStreamingPayloadMsg(req, seq, 0, attachments)
	require.NoError(s.t, err)
	s.send(msg)
}

func (s *fakeServer) sendFeedback(req *rpc.TRequestHeader, readPosition int64) {
	s.t.Helper()

	msg, err := buildStreamingFeedbackMsg(req, readPosition)
	require.NoError(s.t, err)
	s.send(msg)
}

func (s *fakeServer) sendResponse(req *rpc.TRequestHeader, body proto.Message, rspErr *misc.TError) {
	s.t.Helper()

	first, err := marshalMsgHeader(msgResponse, &rpc.TResponseHeader{
		RequestId: req.RequestId,
		Error:     rspErr,
	})
	require.NoError(s.t, err)

	msg := [][]byte{first}
	if rspErr == nil {
		data, err := proto.Marshal(body)
		require.NoError(s.t, err)
		msg = append(msg, data)
	}
	s.send(msg)
}

func newStreamingEcho(t *testing.T, ctx context.Context, client *ClientConn, opts ...SendOption) (*ClientStream, *testservice.TRspStreamingEcho) {
	t.Helper()

	rsp := &testservice.TRspStreamingEcho{}
	stream, err := client.SendStreaming(ctx, "TestService", "StreamingEcho", &testservice.TReqStreamingEcho{}, rsp, opts...)
	require.NoError(t, err)
	return stream, rsp
}

func requireBlocked(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		t.Fatalf("operation is expected to block, finished with %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

func requireDone(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("operation is expected to finish")
		return nil
	}
}

func isRegistered(c *ClientConn, s *ClientStream) bool {
	c.l.Lock()
	defer c.l.Unlock()
	_, ok := c.reqs[s.req.id]
	return ok
}

func TestStreamingMessages(t *testing.T) {
	reqHeader := &rpc.TRequestHeader{
		RequestId: misc.NewProtoFromGUID(guid.New()),
		Service:   ptr.String("TestService"),
		Method:    ptr.String("StreamingEcho"),
		RealmId:   misc.NewProtoFromGUID(guid.New()),
	}

	t.Run("Payload", func(t *testing.T) {
		attachments := [][]byte{[]byte("data"), {}, nil}

		msg, err := buildStreamingPayloadMsg(reqHeader, 7, 0, attachments)
		require.NoError(t, err)
		require.Equal(t, msgStreamPayload, msgType(binary.LittleEndian.Uint32(msg[0][:4])))

		header, parsed, err := parseStreamingPayloadMsg(msg)
		require.NoError(t, err)
		require.True(t, proto.Equal(reqHeader.RequestId, header.RequestId))
		require.True(t, proto.Equal(reqHeader.RealmId, header.RealmId))
		require.Equal(t, "TestService", header.GetService())
		require.Equal(t, "StreamingEcho", header.GetMethod())
		require.Equal(t, int32(7), header.GetSequenceNumber())
		require.Equal(t, int32(0), header.GetCodec())
		require.Equal(t, attachments, parsed)
	})

	t.Run("Feedback", func(t *testing.T) {
		msg, err := buildStreamingFeedbackMsg(reqHeader, 101)
		require.NoError(t, err)
		require.Len(t, msg, 1)
		require.Equal(t, msgStreamFeedback, msgType(binary.LittleEndian.Uint32(msg[0][:4])))

		header, err := parseStreamingFeedbackMsg(msg)
		require.NoError(t, err)
		require.True(t, proto.Equal(reqHeader.RequestId, header.RequestId))
		require.Equal(t, "TestService", header.GetService())
		require.Equal(t, "StreamingEcho", header.GetMethod())
		require.Equal(t, int64(101), header.GetReadPosition())
	})

	t.Run("OverBus", func(t *testing.T) {
		a, b, err := connPair()
		require.NoError(t, err)
		defer func() { _ = a.Close() }()
		defer func() { _ = b.Close() }()

		msg, err := buildStreamingPayloadMsg(reqHeader, 0, 0, [][]byte{[]byte("x"), nil})
		require.NoError(t, err)
		require.NoError(t, NewBus(a, Options{}).Send(guid.New(), msg, &busSendOptions{}))

		transferred, err := NewBus(b, Options{}).Receive()
		require.NoError(t, err)

		_, attachments, err := parseStreamingPayloadMsg(transferred.parts)
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte("x"), nil}, attachments)
	})
}

func TestStreamingWindowSizeOption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := NewClient(ctx, "127.0.0.1:1")
	defer c.Close()
	require.Equal(t, int64(DefaultStreamingWindowSize), c.streamingWindowSize)
	require.Equal(t, int64(16*1024*1024), c.streamingWindowSize)

	c = NewClient(ctx, "127.0.0.1:1", WithStreamingWindowSize(1024))
	defer c.Close()
	require.Equal(t, int64(1024), c.streamingWindowSize)
}

func TestSendStreaming_RequestHeader(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		server, client := newFakeServerConn(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		_, _ = newStreamingEcho(t, ctx, client)

		header := server.recvRequest()
		require.Nil(t, header.Timeout)
		require.NotNil(t, header.ServerAttachmentsStreamingParameters)
		require.Equal(t, int64(16*1024*1024), header.ServerAttachmentsStreamingParameters.GetWindowSize())
		require.Nil(t, header.ServerAttachmentsStreamingParameters.ReadTimeout)
		require.Nil(t, header.ServerAttachmentsStreamingParameters.WriteTimeout)
	})

	t.Run("Timeouts", func(t *testing.T) {
		server, client := newFakeServerConn(t)
		_, _ = newStreamingEcho(t, context.Background(), client,
			WithRequestTimeout(time.Second),
			WithStreamingTimeouts(15*time.Minute, time.Minute))

		header := server.recvRequest()
		require.Nil(t, header.Timeout)
		require.Equal(t, int64(16*1024*1024), header.ServerAttachmentsStreamingParameters.GetWindowSize())
		require.Equal(t, (15 * time.Minute).Microseconds(), header.ServerAttachmentsStreamingParameters.GetReadTimeout())
		require.Equal(t, time.Minute.Microseconds(), header.ServerAttachmentsStreamingParameters.GetWriteTimeout())
	})
}

func TestSendStreaming_Echo(t *testing.T) {
	server, client := newFakeServerConn(t)
	stream, rsp := newStreamingEcho(t, context.Background(), client)
	req := server.recvRequest()

	// Client to server: three blocks and the end of stream.
	for _, data := range []string{"a", "bb", "ccc"} {
		require.NoError(t, stream.Write([]byte(data)))
	}
	require.NoError(t, stream.CloseSend())
	require.NoError(t, stream.CloseSend(), "second CloseSend is a no-op")
	require.Error(t, stream.Write([]byte("late")))

	var received [][]byte
	for i := int32(0); i < 4; i++ {
		header, attachments := server.recvPayload()
		require.Equal(t, i, header.GetSequenceNumber())
		require.Equal(t, "TestService", header.GetService())
		require.Equal(t, "StreamingEcho", header.GetMethod())
		require.True(t, proto.Equal(req.RequestId, header.RequestId))
		received = append(received, attachments...)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), nil}, received)
	server.sendFeedback(req, 7)

	// Server to client: payloads out of order.
	server.sendPayload(req, 1, []byte("world"), nil)
	server.sendPayload(req, 0, []byte("hello"), []byte{})

	data, err := stream.Read()
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), data)

	data, err = stream.Read()
	require.NoError(t, err)
	require.Equal(t, []byte{}, data)

	data, err = stream.Read()
	require.NoError(t, err)
	require.Equal(t, []byte("world"), data)

	_, err = stream.Read()
	require.ErrorIs(t, err, io.EOF)
	_, err = stream.Read()
	require.ErrorIs(t, err, io.EOF)

	// Feedback is coalesced, so wait for the final position: 5 + 1 + 5 + 1.
	for pos := server.recvFeedback(); pos != 12; pos = server.recvFeedback() {
		require.Less(t, pos, int64(12))
	}

	server.sendResponse(req, &testservice.TRspStreamingEcho{TotalSize: ptr.Int64(6)}, nil)
	require.NoError(t, stream.Wait())
	require.Equal(t, int64(6), rsp.GetTotalSize())

	require.Eventually(t, func() bool { return !isRegistered(client, stream) }, 5*time.Second, 10*time.Millisecond)
}

func TestSendStreaming_FeedbackAfterNullAttachment(t *testing.T) {
	server, client := newFakeServerConn(t)
	stream, _ := newStreamingEcho(t, context.Background(), client)
	req := server.recvRequest()

	server.sendPayload(req, 0, make([]byte, 100))
	server.sendPayload(req, 1, nil)

	data, err := stream.Read()
	require.NoError(t, err)
	require.Len(t, data, 100)
	require.Equal(t, int64(100), server.recvFeedback())

	_, err = stream.Read()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(101), server.recvFeedback())
}

func TestSendStreaming_PayloadAfterResponse(t *testing.T) {
	server, client := newFakeServerConn(t)
	stream, _ := newStreamingEcho(t, context.Background(), client)
	req := server.recvRequest()

	server.sendResponse(req, &testservice.TRspStreamingEcho{TotalSize: ptr.Int64(0)}, nil)
	require.NoError(t, stream.Wait())
	require.True(t, isRegistered(client, stream), "request must stay registered until the end of stream")

	server.sendPayload(req, 0, []byte("late"), nil)

	data, err := stream.Read()
	require.NoError(t, err)
	require.Equal(t, []byte("late"), data)

	_, err = stream.Read()
	require.ErrorIs(t, err, io.EOF)

	require.Eventually(t, func() bool { return !isRegistered(client, stream) }, 5*time.Second, 10*time.Millisecond)

	// Writes after a successful response fail.
	require.Error(t, stream.Write([]byte("x")))
}

func TestSendStreaming_Window(t *testing.T) {
	t.Run("BlocksUntilFeedback", func(t *testing.T) {
		server, client := newFakeServerConn(t, WithStreamingWindowSize(10))
		stream, _ := newStreamingEcho(t, context.Background(), client)
		req := server.recvRequest()

		require.NoError(t, stream.Write(make([]byte, 8)))
		server.recvPayload()

		done := make(chan error, 1)
		go func() { done <- stream.Write(make([]byte, 8)) }()
		requireBlocked(t, done)

		// Partial feedback that does not free enough space.
		server.sendFeedback(req, 5)
		requireBlocked(t, done)

		server.sendFeedback(req, 8)
		require.NoError(t, requireDone(t, done))

		_, attachments := server.recvPayload()
		require.Len(t, attachments[0], 8)
	})

	t.Run("OversizedBlockWhenNothingInFlight", func(t *testing.T) {
		server, client := newFakeServerConn(t, WithStreamingWindowSize(10))
		stream, _ := newStreamingEcho(t, context.Background(), client)
		req := server.recvRequest()

		require.NoError(t, stream.Write(make([]byte, 100)))
		_, attachments := server.recvPayload()
		require.Len(t, attachments[0], 100)

		done := make(chan error, 1)
		go func() { done <- stream.Write(make([]byte, 1)) }()
		requireBlocked(t, done)

		server.sendFeedback(req, 100)
		require.NoError(t, requireDone(t, done))
	})

	t.Run("FeedbackBeyondWritePosition", func(t *testing.T) {
		server, client := newFakeServerConn(t)
		stream, _ := newStreamingEcho(t, context.Background(), client)
		req := server.recvRequest()

		require.NoError(t, stream.Write(make([]byte, 8)))
		server.recvPayload()

		server.sendFeedback(req, 9)

		err := stream.Wait()
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeProtocolError))
		require.Error(t, stream.Write([]byte("x")))
		_, err = stream.Read()
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeProtocolError))

		require.Equal(t, msgCancel, server.recv().typ)
	})
}

func TestSendStreaming_ReorderBuffer(t *testing.T) {
	t.Run("GapOverflow", func(t *testing.T) {
		server, client := newFakeServerConn(t)
		stream, _ := newStreamingEcho(t, context.Background(), client)
		req := server.recvRequest()

		server.sendPayload(req, maxStreamingPayloadGap-1, []byte("ok"))
		server.sendPayload(req, maxStreamingPayloadGap, []byte("too far"))

		_, err := stream.Read()
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeProtocolError))
		require.Equal(t, msgCancel, server.recv().typ)
	})

	t.Run("CompressedPayload", func(t *testing.T) {
		server, client := newFakeServerConn(t)
		stream, _ := newStreamingEcho(t, context.Background(), client)
		req := server.recvRequest()

		codec := compression.NewCodec(compression.CodecIDSnappy)
		compressed, err := codec.Compress([]byte("compressed data"))
		require.NoError(t, err)

		msg, err := buildStreamingPayloadMsg(req, 0, int32(compression.CodecIDSnappy), [][]byte{compressed, nil})
		require.NoError(t, err)
		server.send(msg)

		data, err := stream.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("compressed data"), data)

		_, err = stream.Read()
		require.ErrorIs(t, err, io.EOF)
		// Read position counts decompressed bytes; feedback may be coalesced with the end of stream.
		require.Contains(t, []int64{15, 16}, server.recvFeedback())
	})

	t.Run("Duplicate", func(t *testing.T) {
		server, client := newFakeServerConn(t)
		stream, _ := newStreamingEcho(t, context.Background(), client)
		req := server.recvRequest()

		server.sendPayload(req, 0, []byte("a"))
		data, err := stream.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("a"), data)
		require.Equal(t, int64(1), server.recvFeedback())

		server.sendPayload(req, 0, []byte("a"))
		_, err = stream.Read()
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeProtocolError))
	})
}

func TestSendStreaming_ErrorResponse(t *testing.T) {
	server, client := newFakeServerConn(t)
	stream, _ := newStreamingEcho(t, context.Background(), client)
	req := server.recvRequest()

	readDone := make(chan error, 1)
	go func() {
		_, err := stream.Read()
		readDone <- err
	}()
	requireBlocked(t, readDone)

	server.sendResponse(req, nil, &misc.TError{Code: ptr.Int32(500), Message: ptr.String("No such node")})

	err := stream.Wait()
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, 500))

	require.True(t, yterrors.ContainsErrorCode(requireDone(t, readDone), 500))
	require.True(t, yterrors.ContainsErrorCode(stream.Write([]byte("x")), 500))
	_, err = stream.Read()
	require.True(t, yterrors.ContainsErrorCode(err, 500))

	require.Eventually(t, func() bool { return !isRegistered(client, stream) }, 5*time.Second, 10*time.Millisecond)
}

func TestSendStreaming_Abort(t *testing.T) {
	server, client := newFakeServerConn(t)
	stream, _ := newStreamingEcho(t, context.Background(), client)
	req := server.recvRequest()

	abortErr := yterrors.Err("aborted by test")
	stream.Abort(abortErr)

	msg := server.recv()
	require.Equal(t, msgCancel, msg.typ)
	var cancelHeader rpc.TRequestHeader
	require.NoError(t, proto.Unmarshal(msg.parts[0][4:], &cancelHeader))
	require.True(t, proto.Equal(req.RequestId, cancelHeader.RequestId))

	require.ErrorIs(t, stream.Wait(), abortErr)
	require.ErrorIs(t, stream.Write([]byte("x")), abortErr)
	_, err := stream.Read()
	require.ErrorIs(t, err, abortErr)
	require.False(t, isRegistered(client, stream))
}

func TestSendStreaming_ContextCanceled(t *testing.T) {
	t.Run("Canceled", func(t *testing.T) {
		server, client := newFakeServerConn(t, WithStreamingWindowSize(10))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, _ := newStreamingEcho(t, ctx, client)
		server.recvRequest()

		require.NoError(t, stream.Write(make([]byte, 10)))
		server.recvPayload()

		done := make(chan error, 1)
		go func() { done <- stream.Write(make([]byte, 10)) }()
		requireBlocked(t, done)

		cancel()

		err := requireDone(t, done)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeCanceled))
		require.Equal(t, msgCancel, server.recv().typ)
		require.True(t, yterrors.ContainsErrorCode(stream.Wait(), yterrors.CodeCanceled))
	})

	t.Run("DeadlineExceeded", func(t *testing.T) {
		server, client := newFakeServerConn(t)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		stream, _ := newStreamingEcho(t, ctx, client)
		server.recvRequest()

		_, err := stream.Read()
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout))
		require.Equal(t, msgCancel, server.recv().typ)
	})
}

func TestSendStreaming_ConnectionClosed(t *testing.T) {
	server, client := newFakeServerConn(t)
	stream, _ := newStreamingEcho(t, context.Background(), client)
	server.recvRequest()

	server.bus.Close()

	_, err := stream.Read()
	require.ErrorIs(t, err, ErrConnClosed)
	require.ErrorIs(t, stream.Wait(), ErrConnClosed)
}

func TestSendStreaming_UnaryCallAlongside(t *testing.T) {
	server, client := newFakeServerConn(t)
	stream, rsp := newStreamingEcho(t, context.Background(), client)
	streamReq := server.recvRequest()

	unaryRsp := &testservice.TRspStreamingEcho{}
	unaryDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		unaryDone <- client.Send(ctx, "TestService", "SomeCall", &testservice.TReqStreamingEcho{}, unaryRsp)
	}()

	unaryReq := server.recvRequest()
	require.NotNil(t, unaryReq.Timeout, "unary calls keep the request timeout from context")
	require.Nil(t, unaryReq.ServerAttachmentsStreamingParameters)

	require.NoError(t, stream.Write([]byte("data")))
	server.recvPayload()

	server.sendResponse(unaryReq, &testservice.TRspStreamingEcho{TotalSize: ptr.Int64(1)}, nil)
	require.NoError(t, requireDone(t, unaryDone))
	require.Equal(t, int64(1), unaryRsp.GetTotalSize())

	require.NoError(t, stream.CloseSend())
	server.recvPayload()
	server.sendPayload(streamReq, 0, nil)
	_, err := stream.Read()
	require.ErrorIs(t, err, io.EOF)

	server.sendResponse(streamReq, &testservice.TRspStreamingEcho{TotalSize: ptr.Int64(4)}, nil)
	require.NoError(t, stream.Wait())
	require.Equal(t, int64(4), rsp.GetTotalSize())
}
