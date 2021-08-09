package rpcclient

import (
	"context"
	"fmt"

	"a.yandex-team.ru/yt/go/yterrors"
	"github.com/golang/protobuf/proto"
)

type ErrorWrapper struct{}

func annotateError(call *Call, err error) error {
	args := []interface{}{
		// put call_id into message, so in is present even in short error message.
		fmt.Sprintf("call %s failed", call.CallID.String()),
		err,
		yterrors.Attr("method", call.Method),
	}

	if path, ok := call.Req.Path(); ok {
		args = append(args, yterrors.Attr("path", path))
	}

	return yterrors.Err(args...)
}

func (e *ErrorWrapper) Intercept(ctx context.Context, call *Call, next CallInvoker, rsp proto.Message) (err error) {
	err = next(ctx, call, rsp)
	if err != nil {
		return annotateError(call, err)
	}
	return
}
