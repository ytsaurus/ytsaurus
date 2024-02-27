package rpcclient

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type ErrorWrapper struct{}

func annotateError(call *Call, err error) error {
	args := []any{
		// put call_id into message, so it is present even in short error message.
		fmt.Sprintf("call %s failed", call.CallID.String()),
		err,
		yterrors.Attr("method", call.Method),
	}

	if path, ok := call.Req.Path(); ok {
		args = append(args, yterrors.Attr("path", path))
	}

	return yterrors.Err(args...)
}

func (e *ErrorWrapper) Intercept(ctx context.Context, call *Call, next CallInvoker, rsp proto.Message, opts ...bus.SendOption) (err error) {
	err = next(ctx, call, rsp, opts...)
	if err != nil {
		return annotateError(call, err)
	}
	return
}
