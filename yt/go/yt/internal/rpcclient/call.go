package rpcclient

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/proto/client/api/rpc_proxy"
	"a.yandex-team.ru/yt/go/yt"
)

type Call struct {
	Method      Method
	Req         Request
	Attachments [][]byte

	CallID guid.GUID

	RequestedProxy string
	SelectedProxy  string
	Backoff        backoff.BackOff
	DisableRetries bool

	OnRspParams func(b []byte) error
}

type Request interface {
	proto.Message

	Log() []log.Field
	Path() (string, bool)
}

type ProtoRowset interface {
	proto.Message

	GetRowsetDescriptor() *rpc_proxy.TRowsetDescriptor
}

type CallInvoker func(ctx context.Context, call *Call, rsp proto.Message) (err error)

func (c CallInvoker) Wrap(interceptor CallInterceptor) CallInvoker {
	return func(ctx context.Context, call *Call, rsp proto.Message) (err error) {
		return interceptor(ctx, call, c, rsp)
	}
}

type CallInterceptor func(ctx context.Context, call *Call, invoke CallInvoker, rsp proto.Message) (err error)

type ReadRowInvoker func(ctx context.Context, call *Call, rsp ProtoRowset) (r yt.TableReader, err error)

func (c ReadRowInvoker) Wrap(interceptor ReadRowInterceptor) ReadRowInvoker {
	return func(ctx context.Context, call *Call, rsp ProtoRowset) (r yt.TableReader, err error) {
		return interceptor(ctx, call, c, rsp)
	}
}

type ReadRowInterceptor func(ctx context.Context, call *Call, invoke ReadRowInvoker, rsp ProtoRowset) (r yt.TableReader, err error)
