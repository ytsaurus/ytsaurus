package rpcclient

import (
	"context"

	"github.com/golang/protobuf/proto"

	"a.yandex-team.ru/yt/go/bus"
)

type BusConn interface {
	// Send performs a unary RPC and returns after the response is received into reply.
	Send(
		ctx context.Context,
		service, method string,
		request, reply proto.Message,
		opts ...bus.SendOption,
	) error
}

type Dialer func(ctx context.Context, addr string) (BusConn, error)

type dialerKey struct{}

func WithDialer(ctx context.Context, dialer Dialer) context.Context {
	return context.WithValue(ctx, dialerKey{}, dialer)
}

func GetDialer(ctx context.Context) (Dialer, bool) {
	v := ctx.Value(dialerKey{})
	if v == nil {
		return nil, false
	}

	return v.(Dialer), true
}

func DefaultDial(ctx context.Context, addr string) (BusConn, error) {
	return bus.NewClient(ctx, addr, bus.WithDefaultProtocolVersionMajor(ProtocolVersionMajor))
}
