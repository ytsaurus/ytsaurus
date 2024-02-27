package internal

import (
	"context"
	"io"

	"github.com/cenkalti/backoff/v4"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

type Params interface {
	HTTPVerb() Verb

	YPath() (ypath.YPath, bool)
	Log() []log.Field

	MarshalHTTP(w *yson.Writer)
}

type Call struct {
	Params Params
	CallID guid.GUID

	YSONValue []byte
	RowBatch  yt.RowBatch

	RequestedProxy string
	SelectedProxy  string
	Backoff        backoff.BackOff
	DisableRetries bool

	OnRspParams func(b []byte) error
}

type CallResult struct {
	YSONValue []byte
}

func (res *CallResult) decodeSingle(key string, value any) (err error) {
	err = yson.Unmarshal(res.YSONValue, &unmapper{key: key, value: value})
	return
}

func (res *CallResult) decodeValue(value any) (err error) {
	return res.decodeSingle("value", value)
}

func (res *CallResult) decode(value any) (err error) {
	err = yson.Unmarshal(res.YSONValue, value)
	return
}

type CallInvoker func(ctx context.Context, call *Call) (res *CallResult, err error)

func (c CallInvoker) Wrap(interceptor CallInterceptor) CallInvoker {
	return func(ctx context.Context, call *Call) (res *CallResult, err error) {
		return interceptor(ctx, call, c)
	}
}

type CallInterceptor func(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error)

type ReadInvoker func(ctx context.Context, call *Call) (r io.ReadCloser, err error)

func (c ReadInvoker) Wrap(interceptor ReadInterceptor) ReadInvoker {
	return func(ctx context.Context, call *Call) (r io.ReadCloser, err error) {
		return interceptor(ctx, call, c)
	}
}

type ReadInterceptor func(ctx context.Context, call *Call, invoke ReadInvoker) (r io.ReadCloser, err error)

type WriteInvoker func(ctx context.Context, call *Call) (r io.WriteCloser, err error)

func (c WriteInvoker) Wrap(interceptor WriteInterceptor) WriteInvoker {
	return func(ctx context.Context, call *Call) (r io.WriteCloser, err error) {
		return interceptor(ctx, call, c)
	}
}

type WriteInterceptor func(ctx context.Context, call *Call, invoke WriteInvoker) (r io.WriteCloser, err error)

type ReadRowInvoker func(ctx context.Context, call *Call) (r yt.TableReader, err error)

func (c ReadRowInvoker) Wrap(interceptor ReadRowInterceptor) ReadRowInvoker {
	return func(ctx context.Context, call *Call) (r yt.TableReader, err error) {
		return interceptor(ctx, call, c)
	}
}

type ReadRowInterceptor func(ctx context.Context, call *Call, invoke ReadRowInvoker) (r yt.TableReader, err error)

type WriteRowInvoker func(ctx context.Context, call *Call) (r yt.TableWriter, err error)

func (c WriteRowInvoker) Wrap(interceptor WriteRowInterceptor) WriteRowInvoker {
	return func(ctx context.Context, call *Call) (r yt.TableWriter, err error) {
		return interceptor(ctx, call, c)
	}
}

type WriteRowInterceptor func(ctx context.Context, call *Call, invoke WriteRowInvoker) (r yt.TableWriter, err error)
