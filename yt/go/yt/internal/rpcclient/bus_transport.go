package rpcclient

import (
	"context"
	"errors"
	"net/http"

	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/yt"
)

type busTransport struct {
	connPool *ConnPool
	conf     *yt.Config
}

var _ transport = (*busTransport)(nil)

func newBusTransport(c *client, httpTransport *http.Transport) *busTransport {
	conf := c.conf
	return &busTransport{
		connPool: NewConnPool(func(ctx context.Context, addr string) BusConn {
			clientOpts := []bus.ClientOption{
				bus.WithLogger(c.log.Logger()),
				bus.WithDefaultProtocolVersionMajor(ProtocolVersionMajor),
				bus.WithNetwork(conf.GetIPVersion().Network()),
			}
			if conf.UseTLS && httpTransport != nil && httpTransport.TLSClientConfig != nil {
				busTLSConfig := httpTransport.TLSClientConfig.Clone()
				busTLSConfig.ServerName = resolveServerName(conf, addr)
				clientOpts = append(clientOpts, bus.WithEncryptionMode(bus.EncryptionModeRequired))
				clientOpts = append(clientOpts, bus.WithTLSConfig(busTLSConfig))
			}
			return bus.NewClient(ctx, addr, clientOpts...)
		}, c.log),
		conf: conf,
	}
}

func (t *busTransport) ProxyType() string { return "rpc" }

func (t *busTransport) Discard(addr string) {
	t.connPool.Discard(addr)
}

func (t *busTransport) Stop() {
	t.connPool.Stop()
}

func (t *busTransport) Send(
	ctx context.Context,
	addr string,
	call *Call,
	rsp proto.Message,
	creds yt.Credentials,
) ([][]byte, error) {
	var rspAttachments [][]byte
	opts := []bus.SendOption{
		bus.WithRequestID(call.CallID),
	}
	if creds != nil {
		opts = append(opts, bus.WithCredentials(creds))
	}
	if call.Attachments != nil {
		opts = append(opts, bus.WithAttachments(call.Attachments...))
	}
	t.injectTracing(ctx, &opts)
	opts = append(opts, bus.WithResponseAttachments(&rspAttachments))

	conn, err := t.getConn(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	err = conn.Send(ctx, "ApiService", string(call.Method), call.Req, rsp, opts...)
	if errors.Is(err, bus.ErrConnClosed) {
		conn.Discard()
	}
	return rspAttachments, err
}

func (t *busTransport) getConn(ctx context.Context, addr string) (*Conn, error) {
	dial, ok := GetDialer(ctx)
	if ok {
		conn := dial(ctx, addr)
		wrapped := NewConn(addr, conn, nil)
		return wrapped, nil
	}
	return t.connPool.Conn(ctx, addr)
}

func (t *busTransport) injectTracing(ctx context.Context, opts *[]bus.SendOption) {
	if t.conf.TraceFn == nil {
		return
	}

	traceID, spanID, flags, ok := t.conf.TraceFn(ctx)
	if !ok {
		return
	}

	*opts = append(*opts, bus.WithTracing(traceID, spanID, flags))
}
