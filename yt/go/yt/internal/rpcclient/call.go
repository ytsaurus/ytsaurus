package rpcclient

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/yt"
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
}

type Request interface {
	proto.Message

	Log() []log.Field
	Path() (string, bool)
}

type ProtoMultiLookupResp interface {
	proto.Message

	GetSubresponses() []ProtoMultiLookupSubresp
}

type ProtoMultiLookupSubresp interface {
	ProtoRowset

	GetAttachmentCount() int32
	GetUnavailableKeyIndexes() []int32
}

type ProtoRowset interface {
	proto.Message

	GetRowsetDescriptor() *rpc_proxy.TRowsetDescriptor
}

type CallInvoker func(ctx context.Context, call *Call, rsp proto.Message, opts ...bus.SendOption) (err error)

func (c CallInvoker) Wrap(interceptor CallInterceptor) CallInvoker {
	return func(ctx context.Context, call *Call, rsp proto.Message, opts ...bus.SendOption) (err error) {
		return interceptor(ctx, call, c, rsp, opts...)
	}
}

type CallInterceptor func(ctx context.Context, call *Call, invoke CallInvoker, rsp proto.Message, opts ...bus.SendOption) (err error)

// ClientStream is a streaming call, see bus.ClientStream.
type ClientStream interface {
	Write(data []byte) error
	CloseSend() error
	Read() ([]byte, error)
	Wait() error
	Abort(err error)
}

type StreamInvoker func(ctx context.Context, call *Call, rsp proto.Message, opts ...bus.SendOption) (stream ClientStream, err error)

func (c StreamInvoker) Wrap(interceptor StreamInterceptor) StreamInvoker {
	return func(ctx context.Context, call *Call, rsp proto.Message, opts ...bus.SendOption) (stream ClientStream, err error) {
		return interceptor(ctx, call, c, rsp, opts...)
	}
}

type StreamInterceptor func(ctx context.Context, call *Call, invoke StreamInvoker, rsp proto.Message, opts ...bus.SendOption) (stream ClientStream, err error)

type ReadRowInvoker func(ctx context.Context, call *Call, rsp ProtoRowset) (r yt.TableReader, err error)

func (c ReadRowInvoker) Wrap(interceptor ReadRowInterceptor) ReadRowInvoker {
	return func(ctx context.Context, call *Call, rsp ProtoRowset) (r yt.TableReader, err error) {
		return interceptor(ctx, call, c, rsp)
	}
}

type ReadRowInterceptor func(ctx context.Context, call *Call, invoke ReadRowInvoker, rsp ProtoRowset) (r yt.TableReader, err error)

type MultiLookupInvoker func(ctx context.Context, call *Call, rsp ProtoMultiLookupResp) (readers []yt.TableReader, err error)

func (c MultiLookupInvoker) Wrap(interceptor MultiLookupInterceptor) MultiLookupInvoker {
	return func(ctx context.Context, call *Call, rsp ProtoMultiLookupResp) (readers []yt.TableReader, err error) {
		return interceptor(ctx, call, c, rsp)
	}
}

type MultiLookupInterceptor func(ctx context.Context, call *Call, invoke MultiLookupInvoker, rsp ProtoMultiLookupResp) (readers []yt.TableReader, err error)
