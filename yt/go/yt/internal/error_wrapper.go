package internal

import (
	"context"
	"fmt"
	"io"

	"go.ytsaurus.tech/yt/go/yterrors"
)

type ErrorWrapper struct{}

func annotateError(call *Call, err error) error {
	args := []any{
		// put call_id into message, so in is present even in short error message.
		fmt.Sprintf("call %s failed", call.CallID.String()),
		err,
		yterrors.Attr("method", call.Params.HTTPVerb()),
	}

	if path, ok := call.Params.YPath(); ok {
		args = append(args, yterrors.Attr("path", path.YPath()))
	}

	return yterrors.Err(args...)
}

func (e *ErrorWrapper) Intercept(ctx context.Context, call *Call, next CallInvoker) (res *CallResult, err error) {
	res, err = next(ctx, call)
	if err != nil {
		return nil, annotateError(call, err)
	}
	return
}

func (e *ErrorWrapper) Read(ctx context.Context, call *Call, next ReadInvoker) (r io.ReadCloser, err error) {
	r, err = next(ctx, call)
	if err != nil {
		return nil, annotateError(call, err)
	}

	r = &readErrorWrapper{call: call, r: r}
	return
}

func (e *ErrorWrapper) Write(ctx context.Context, call *Call, next WriteInvoker) (w io.WriteCloser, err error) {
	w, err = next(ctx, call)
	if err != nil {
		return nil, annotateError(call, err)
	}

	w = &writeErrorWrapper{call: call, w: w}
	return
}

type readErrorWrapper struct {
	call *Call
	r    io.ReadCloser
}

func (w *readErrorWrapper) Read(p []byte) (n int, err error) {
	n, err = w.r.Read(p)
	if err != nil && err != io.EOF {
		err = annotateError(w.call, err)
	}
	return
}

func (w *readErrorWrapper) Close() error {
	// No point of wrapping this error. This method is to signal that client is done reading the body.
	return w.r.Close()
}

type writeErrorWrapper struct {
	call *Call
	w    io.WriteCloser
}

func (w *writeErrorWrapper) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	if err != nil {
		err = annotateError(w.call, err)
	}
	return
}

func (w *writeErrorWrapper) Close() error {
	err := w.w.Close()
	if err != nil {
		err = annotateError(w.call, err)
	}
	return err
}
