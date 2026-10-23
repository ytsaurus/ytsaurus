package rpcclient

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type fakeRead struct {
	data []byte
	err  error
}

// fakeStream is a scriptable ClientStream.
type fakeStream struct {
	mu sync.Mutex

	// reads are returned by Read in order, io.EOF is returned when they are exhausted.
	reads []fakeRead
	// writes records written attachments, nil stands for CloseSend.
	writes [][]byte

	writeErr error
	waitErr  error

	waitCalls int
	abortErr  error
	aborted   bool
}

func (s *fakeStream) Write(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writeErr != nil {
		return s.writeErr
	}
	s.writes = append(s.writes, data)
	return nil
}

func (s *fakeStream) CloseSend() error {
	return s.Write(nil)
}

func (s *fakeStream) Read() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.aborted {
		return nil, s.abortErr
	}
	if len(s.reads) == 0 {
		return nil, io.EOF
	}

	r := s.reads[0]
	s.reads = s.reads[1:]
	return r.data, r.err
}

func (s *fakeStream) Wait() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.waitCalls++
	if s.aborted && s.waitErr == nil {
		return s.abortErr
	}
	return s.waitErr
}

func (s *fakeStream) Abort(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.aborted {
		s.aborted = true
		s.abortErr = err
	}
}

func newTestCall(req Request) *Call {
	return &Call{Method: MethodReadFile, Req: req, CallID: guid.New()}
}

func newTestStreamChain(invoke StreamInvoker) StreamInvoker {
	return wrapStreamInvoker(invoke,
		&ProxyBouncer{Log: &nop.Logger{}},
		&LoggingInterceptor{Structured: &nop.Logger{}},
		&TracingInterceptor{Tracer: opentracing.NoopTracer{}},
		&ErrorWrapper{})
}

func TestFinishingStream(t *testing.T) {
	stream := &fakeStream{waitErr: yterrors.Err("final error")}

	var finishCalls int
	s := newFinishingStream(stream, func(err error) error {
		finishCalls++
		return yterrors.Err("wrapped", err)
	})

	err := s.Wait()
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrapped")

	require.Equal(t, err, s.Wait())
	s.Abort(yterrors.Err("abort"))
	require.Equal(t, err, s.Wait())

	require.Equal(t, 1, finishCalls, "onFinish must be called once")
	require.True(t, stream.aborted)
}

func TestFinishingStream_Abort(t *testing.T) {
	stream := &fakeStream{}

	var finishErr error
	var finishCalls int
	s := newFinishingStream(stream, func(err error) error {
		finishCalls++
		finishErr = err
		return err
	})

	abortErr := yterrors.Err(yterrors.CodeCanceled, "canceled")
	s.Abort(abortErr)

	require.Equal(t, 1, finishCalls, "Abort must finish the stream")
	require.ErrorIs(t, finishErr, abortErr)
}

func TestStreamInvoker_NoRetries(t *testing.T) {
	req := NewReadFileRequest(&rpc_proxy.TReqReadFile{Path: []byte("//tmp/file")})

	t.Run("StartError", func(t *testing.T) {
		var calls int
		invoke := newTestStreamChain(func(ctx context.Context, call *Call, rsp proto.Message, opts ...bus.SendOption) (ClientStream, error) {
			calls++
			// NB: This error is retried by Retrier for unary calls.
			return nil, yterrors.Err(yterrors.CodeTimeout, "timeout")
		})

		call := newTestCall(req)
		_, err := invoke(context.Background(), call, &rpc_proxy.TRspReadFile{})
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout))
		require.Contains(t, err.Error(), call.CallID.String(), "error must be annotated with call id")
		require.Equal(t, 1, calls, "streaming call must not be retried")
	})

	t.Run("FinalError", func(t *testing.T) {
		var calls int
		base := &fakeStream{waitErr: yterrors.Err(yterrors.CodeTimeout, "timeout")}
		invoke := newTestStreamChain(func(ctx context.Context, call *Call, rsp proto.Message, opts ...bus.SendOption) (ClientStream, error) {
			calls++
			return base, nil
		})

		call := newTestCall(req)
		stream, err := invoke(context.Background(), call, &rpc_proxy.TRspReadFile{})
		require.NoError(t, err)

		err = stream.Wait()
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout))
		require.Contains(t, err.Error(), call.CallID.String(), "error must be annotated with call id")
		require.Equal(t, 1, calls)
		require.Equal(t, 1, base.waitCalls, "chain must wait for the underlying stream once")
	})
}

func TestTxInterceptor_InterceptStream(t *testing.T) {
	txID := yt.TxID(guid.New())
	tx := &TxInterceptor{
		pinger: internal.NewPinger(context.Background(), nil, txID, time.Minute, time.Minute, internal.NewStopGroup(), nil),
	}

	req := NewWriteFileRequest(&rpc_proxy.TReqWriteFile{Path: []byte("//tmp/file")})

	var invokedReq *WriteFileRequest
	var next StreamInvoker = func(ctx context.Context, call *Call, rsp proto.Message, opts ...bus.SendOption) (ClientStream, error) {
		invokedReq = call.Req.(*WriteFileRequest)
		return &fakeStream{}, nil
	}

	_, err := next.Wrap(tx.InterceptStream)(context.Background(), newTestCall(req), &rpc_proxy.TRspWriteFile{})
	require.NoError(t, err)

	require.NotNil(t, invokedReq)
	require.NotNil(t, invokedReq.TransactionalOptions)
	require.Equal(t, guid.GUID(txID), makeGUID(invokedReq.TransactionalOptions.TransactionId))
}
