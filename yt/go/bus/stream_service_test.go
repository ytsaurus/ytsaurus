package bus

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	testservice "go.ytsaurus.tech/yt/go/proto/core/rpc/unittests"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func (c *testServiceClient) sendStreaming(ctx context.Context, method string, req, rsp proto.Message) (*ClientStream, error) {
	return c.conn.SendStreaming(ctx, "TestService", method, req, rsp, WithProtocolVersionMajor(1))
}

// readAll reads the server-to-client stream until the end.
func readAll(stream *ClientStream) ([]byte, error) {
	var buf bytes.Buffer
	for {
		data, err := stream.Read()
		if err == io.EOF {
			return buf.Bytes(), nil
		}
		if err != nil {
			return buf.Bytes(), err
		}
		buf.Write(data)
	}
}

func TestTestService_StreamingEcho(t *testing.T) {
	addr, stop := StartTestService(t)
	defer stop()

	const windowSize = 64 * 1024
	c := NewTestServiceClient(addr, WithStreamingWindowSize(windowSize))
	defer c.Close()

	data := make([]byte, 16*windowSize)
	_, err := rand.Read(data)
	require.NoError(t, err)

	for _, delayed := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		rsp := &testservice.TRspStreamingEcho{}
		stream, err := c.sendStreaming(ctx, "StreamingEcho", &testservice.TReqStreamingEcho{Delayed: ptr.Bool(delayed)}, rsp)
		require.NoError(t, err)

		type result struct {
			data []byte
			err  error
		}
		echo := make(chan result, 1)
		go func() {
			data, err := readAll(stream)
			echo <- result{data, err}
		}()

		const blockSize = windowSize / 4
		for offset := 0; offset < len(data); offset += blockSize {
			require.NoError(t, stream.Write(data[offset:offset+blockSize]))
		}
		require.NoError(t, stream.CloseSend())

		res := <-echo
		require.NoError(t, res.err)
		require.True(t, bytes.Equal(data, res.data), "echoed data differs, delayed=%v", delayed)

		require.NoError(t, stream.Wait())
		require.Equal(t, int64(len(data)), rsp.GetTotalSize())
	}
}

func TestTestService_ServerStreamsAborted(t *testing.T) {
	addr, stop := StartTestService(t)
	defer stop()

	c := NewTestServiceClient(addr)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	stream, err := c.sendStreaming(ctx, "ServerStreamsAborted", &testservice.TReqServerStreamsAborted{}, &testservice.TRspServerStreamsAborted{})
	require.NoError(t, err)

	err = stream.Wait()
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout), err)

	_, err = stream.Read()
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout), err)
	require.True(t, yterrors.ContainsErrorCode(stream.Write([]byte("x")), yterrors.CodeTimeout))
}

func TestTestService_ServerNotReading(t *testing.T) {
	addr, stop := StartTestService(t)
	defer stop()

	c := NewTestServiceClient(addr)
	defer c.Close()

	for _, sleep := range []bool{false, true} {
		// NB: C++ client uses client-side stall timeouts here, Go client relies on the context deadline.
		timeout := 5 * time.Second
		if sleep {
			timeout = 250 * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		stream, err := c.sendStreaming(ctx, "ServerNotReading", &testservice.TReqServerNotReading{Sleep: ptr.Bool(sleep)}, &testservice.TRspServerNotReading{})
		require.NoError(t, err)

		require.NoError(t, stream.Write([]byte("hello")))
		require.NoError(t, stream.CloseSend())

		err = stream.Wait()
		if sleep {
			require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout), err)
			require.True(t, yterrors.ContainsErrorCode(stream.Write([]byte("x")), yterrors.CodeTimeout))
		} else {
			require.NoError(t, err)
		}
	}
}

func TestTestService_ServerNotWriting(t *testing.T) {
	addr, stop := StartTestService(t)
	defer stop()

	c := NewTestServiceClient(addr)
	defer c.Close()

	for _, sleep := range []bool{false, true} {
		// NB: C++ client uses client-side stall timeouts here, Go client relies on the context deadline.
		timeout := 5 * time.Second
		if sleep {
			timeout = 250 * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		stream, err := c.sendStreaming(ctx, "ServerNotWriting", &testservice.TReqServerNotWriting{Sleep: ptr.Bool(sleep)}, &testservice.TRspServerNotWriting{})
		require.NoError(t, err)
		require.NoError(t, stream.CloseSend())

		data, err := stream.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("abacaba"), data)

		_, err = stream.Read()
		if sleep {
			require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout), err)
			require.True(t, yterrors.ContainsErrorCode(stream.Wait(), yterrors.CodeTimeout))
		} else {
			require.ErrorIs(t, err, io.EOF)
			require.NoError(t, stream.Wait())
		}
	}
}

func TestTestService_UnaryCallAlongsideStreaming(t *testing.T) {
	addr, stop := StartTestService(t)
	defer stop()

	c := NewTestServiceClient(addr)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	rsp := &testservice.TRspStreamingEcho{}
	stream, err := c.sendStreaming(ctx, "StreamingEcho", &testservice.TReqStreamingEcho{}, rsp)
	require.NoError(t, err)

	require.NoError(t, stream.Write([]byte("hello")))
	data, err := stream.Read()
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), data)

	someRsp, err := c.SomeCall(ctx, &testservice.TReqSomeCall{A: ptr.Int32(42)})
	require.NoError(t, err)
	require.Equal(t, int32(142), someRsp.GetB())

	require.NoError(t, stream.Write([]byte("world")))
	require.NoError(t, stream.CloseSend())

	rest, err := readAll(stream)
	require.NoError(t, err)
	require.Equal(t, []byte("world"), rest)

	require.NoError(t, stream.Wait())
	require.Equal(t, int64(10), rsp.GetTotalSize())
}
