package rpcclient

import (
	"context"
	"errors"
	"sync"

	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/bus"
)

// streamingBusConn is implemented by connections that support streaming calls, e.g. *bus.ClientConn.
type streamingBusConn interface {
	SendStreaming(
		ctx context.Context,
		service, method string,
		request, reply proto.Message,
		opts ...bus.SendOption,
	) (*bus.ClientStream, error)
}

// finishingStream calls onFinish once with the final error of the stream.
//
// The error returned by onFinish is returned from Wait.
type finishingStream struct {
	ClientStream

	onFinish func(err error) error

	once sync.Once
	err  error
}

func newFinishingStream(stream ClientStream, onFinish func(err error) error) *finishingStream {
	return &finishingStream{ClientStream: stream, onFinish: onFinish}
}

func (s *finishingStream) Wait() error {
	err := s.ClientStream.Wait()
	s.once.Do(func() {
		s.err = s.onFinish(err)
	})
	return s.err
}

func (s *finishingStream) Abort(err error) {
	s.ClientStream.Abort(err)
	// NB: Wait does not block after Abort.
	_ = s.Wait()
}

func (c *client) invokeStream(
	ctx context.Context,
	call *Call,
	rsp proto.Message,
	opts ...bus.SendOption,
) (ClientStream, error) {
	addr := call.RequestedProxy
	if addr == "" {
		var err error
		addr, err = c.pickRPCProxy(ctx)
		if err != nil {
			return nil, err
		}
	}
	call.SelectedProxy = addr

	opts = append(opts,
		bus.WithRequestID(call.CallID),
	)

	credentials, err := c.requestCredentials(ctx)
	if err != nil {
		return nil, err
	}
	if credentials != nil {
		opts = append(opts, bus.WithCredentials(credentials))
	}

	c.injectTracing(ctx, &opts)

	conn, err := c.getConn(ctx, addr)
	if err != nil {
		return nil, err
	}

	releaseConn := func(err error) {
		if errors.Is(err, bus.ErrConnClosed) {
			conn.Discard()
		} else {
			conn.Release()
		}
	}

	streamingConn, ok := conn.BusConn.(streamingBusConn)
	if !ok {
		conn.Release()
		return nil, xerrors.Errorf("rpc connection %T does not support streaming calls", conn.BusConn)
	}

	ctxlog.Debug(ctx, c.log.Logger(), "sending streaming RPC request",
		log.String("proxy", call.SelectedProxy),
		log.String("request_id", call.CallID.String()),
	)

	stream, err := streamingConn.SendStreaming(ctx, "ApiService", string(call.Method), call.Req, rsp, opts...)
	if err != nil {
		releaseConn(err)
		return nil, err
	}

	// NB: The connection is held until the call is finished.
	return newFinishingStream(stream, func(err error) error {
		ctxlog.Debug(ctx, c.log.Logger(), "streaming RPC request finished",
			log.String("proxy", call.SelectedProxy),
			log.String("request_id", call.CallID.String()),
			log.Bool("ok", err == nil))

		releaseConn(err)
		return err
	}), nil
}

func (b *ProxyBouncer) InterceptStream(ctx context.Context, call *Call, next StreamInvoker, rsp proto.Message, opts ...bus.SendOption) (ClientStream, error) {
	stream, err := next(ctx, call, rsp, opts...)
	if err != nil {
		b.banProxy(call, err)
		return nil, err
	}

	return newFinishingStream(stream, func(err error) error {
		b.banProxy(call, err)
		return err
	}), nil
}

func (l *LoggingInterceptor) InterceptStream(ctx context.Context, call *Call, invoke StreamInvoker, rsp proto.Message, opts ...bus.SendOption) (ClientStream, error) {
	ctx = l.logStart(ctx, call)
	stream, err := invoke(ctx, call, rsp, opts...)
	if err != nil {
		l.logFinish(ctx, err)
		return nil, err
	}

	return newFinishingStream(stream, func(err error) error {
		l.logFinish(ctx, err)
		return err
	}), nil
}

func (t *TracingInterceptor) InterceptStream(ctx context.Context, call *Call, invoke StreamInvoker, rsp proto.Message, opts ...bus.SendOption) (ClientStream, error) {
	span, ctx := t.traceStart(ctx, call)
	stream, err := invoke(ctx, call, rsp, opts...)
	if err != nil {
		t.traceFinish(span, err)
		return nil, err
	}

	return newFinishingStream(stream, func(err error) error {
		t.traceFinish(span, err)
		return err
	}), nil
}

func (e *ErrorWrapper) InterceptStream(ctx context.Context, call *Call, next StreamInvoker, rsp proto.Message, opts ...bus.SendOption) (ClientStream, error) {
	stream, err := next(ctx, call, rsp, opts...)
	if err != nil {
		return nil, annotateError(call, err)
	}

	return newFinishingStream(stream, func(err error) error {
		if err != nil {
			return annotateError(call, err)
		}
		return nil
	}), nil
}

func (t *TxInterceptor) InterceptStream(ctx context.Context, call *Call, next StreamInvoker, rsp proto.Message, opts ...bus.SendOption) (ClientStream, error) {
	if err := t.setTxID(call); err != nil {
		return nil, err
	}

	return next(ctx, call, rsp, opts...)
}

// wrapStreamInvoker builds the interceptor chain for streaming calls.
//
// Streaming calls are never retried, since a partly sent stream cannot be replayed.
func wrapStreamInvoker(
	invoke StreamInvoker,
	proxyBouncer *ProxyBouncer,
	requestLogger *LoggingInterceptor,
	requestTracer *TracingInterceptor,
	errorWrapper *ErrorWrapper,
) StreamInvoker {
	return invoke.
		Wrap(proxyBouncer.InterceptStream).
		Wrap(requestLogger.InterceptStream).
		Wrap(requestTracer.InterceptStream).
		Wrap(errorWrapper.InterceptStream)
}
