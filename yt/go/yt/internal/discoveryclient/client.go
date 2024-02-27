package discoveryclient

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal"
	"go.ytsaurus.tech/yt/go/yt/internal/rpcclient"
)

const ProtocolVersionMajor = 0

var _ yt.DiscoveryClient = (*client)(nil)

type client struct {
	Encoder

	conf *yt.DiscoveryConfig

	log log.Structured

	connPool   *rpcclient.ConnPool
	discoverer yt.Discoverer

	proxySet *internal.ProxySet
}

func New(
	conf *yt.DiscoveryConfig,
	discoverer yt.Discoverer,
) (*client, error) {
	c := &client{
		conf:       conf,
		log:        conf.GetLogger(),
		discoverer: discoverer,
	}

	c.connPool = rpcclient.NewConnPool(func(ctx context.Context, addr string) rpcclient.BusConn {
		clientOpts := []bus.ClientOption{
			bus.WithLogger(c.log.Logger()),
			bus.WithDefaultProtocolVersionMajor(ProtocolVersionMajor),
		}
		return bus.NewClient(ctx, addr, clientOpts...)
	}, c.log)

	c.proxySet = &internal.ProxySet{UpdateFn: discoverer.ListDiscoveryServers}

	c.Encoder.StartCall = c.startCall
	c.Encoder.Invoke = c.invoke

	proxyBouncer := &rpcclient.ProxyBouncer{Log: c.log, ProxySet: c.proxySet, ConnPool: c.connPool}
	requestLogger := &rpcclient.LoggingInterceptor{Structured: c.log}
	readRetrier := &rpcclient.Retrier{RequestTimeout: conf.GetRequestTimeout(), Log: c.log}
	errorWrapper := &rpcclient.ErrorWrapper{}

	c.Encoder.Invoke = c.Encoder.Invoke.
		Wrap(proxyBouncer.Intercept).
		Wrap(requestLogger.Intercept).
		Wrap(readRetrier.Intercept).
		Wrap(errorWrapper.Intercept)

	return c, nil
}

func (c *client) startCall() *rpcclient.Call {
	bf := backoff.NewExponentialBackOff()
	bf.MaxElapsedTime = c.conf.GetRequestTimeout()
	return &rpcclient.Call{
		Backoff: bf,
	}
}

func (c *client) invoke(
	ctx context.Context,
	call *rpcclient.Call,
	rsp proto.Message,
	opts ...bus.SendOption,
) error {
	addr, err := c.pickDiscoveryServer(ctx)
	if err != nil {
		return err
	}
	call.SelectedProxy = addr

	opts = append(opts,
		bus.WithRequestID(call.CallID),
	)

	credentials, err := c.requestCredentials(ctx)
	if err != nil {
		return err
	}
	opts = append(opts, bus.WithCredentials(credentials))

	if call.Attachments != nil {
		opts = append(opts, bus.WithAttachments(call.Attachments...))
	}

	conn, err := c.getConn(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Release()

	ctxlog.Debug(ctx, c.log.Logger(), "sending RPC request",
		log.String("server", call.SelectedProxy),
		log.String("request_id", call.CallID.String()),
	)

	ctxlog.Debug(ctx, c.log.Logger(), "User request", log.Any("request", call.Req))
	start := time.Now()
	err = conn.Send(ctx, "DiscoveryClientService", string(call.Method), call.Req, rsp, opts...)
	duration := time.Since(start)

	ctxlog.Debug(ctx, c.log.Logger(), "received RPC response",
		log.String("server", call.SelectedProxy),
		log.String("request_id", call.CallID.String()),
		log.Bool("ok", err == nil),
		log.Duration("duration", duration),
		log.Any("rsp", rsp))

	if errors.Is(err, bus.ErrConnClosed) {
		conn.Discard()
	}

	return err
}

func (c *client) requestCredentials(ctx context.Context) (yt.Credentials, error) {
	if creds := yt.ContextCredentials(ctx); creds != nil {
		return creds, nil
	}

	credentials := &yt.TokenCredentials{Token: c.conf.GetToken()}
	return credentials, nil
}

func (c *client) getConn(ctx context.Context, addr string) (*rpcclient.Conn, error) {
	dial, ok := rpcclient.GetDialer(ctx)
	if ok {
		conn := dial(ctx, addr)
		wrapped := rpcclient.NewConn(addr, conn, nil)
		return wrapped, nil
	}
	return c.connPool.Conn(ctx, addr)
}

func (c *client) Stop() {
	c.connPool.Stop()
	c.discoverer.Stop()
}
