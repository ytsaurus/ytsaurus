package internal

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"go.uber.org/atomic"
)

type tracingReader struct {
	span      opentracing.Span
	t         *TracingInterceptor
	r         io.ReadCloser
	call      *Call
	byteCount int64

	logged atomic.Bool
}

func (r *tracingReader) traceFinish(err error) {
	if !r.logged.Swap(true) {
		r.t.traceFinish(r.span, err, opentracing.Tag{Key: "bytes_read", Value: r.byteCount})
	}
}

func (r *tracingReader) Close() error {
	r.traceFinish(errors.New("request interrupted"))

	return r.r.Close()
}

func (r *tracingReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	r.byteCount += int64(n)

	if err != nil {
		if err == io.EOF {
			r.traceFinish(nil)
		} else {
			r.traceFinish(err)
		}
	}

	return
}

type tracingWriter struct {
	span      opentracing.Span
	t         *TracingInterceptor
	w         io.WriteCloser
	call      *Call
	byteCount int64

	logged atomic.Bool
}

func (w *tracingWriter) traceFinish(err error) {
	if !w.logged.Swap(true) {
		w.t.traceFinish(w.span, err, opentracing.Tag{Key: "bytes_written", Value: w.byteCount})
	}
}

func (w *tracingWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.byteCount += int64(n)

	if err != nil {
		w.traceFinish(err)
	}

	return
}

func (w *tracingWriter) Close() error {
	err := w.w.Close()
	w.traceFinish(err)
	return err
}

type TracingInterceptor struct {
	opentracing.Tracer
}

func (t *TracingInterceptor) traceStart(ctx context.Context, call *Call) (span opentracing.Span, retCtx context.Context) {
	span, retCtx = opentracing.StartSpanFromContextWithTracer(ctx, t.Tracer, string(call.Params.HTTPVerb()), ext.SpanKindRPCClient)
	ext.Component.Set(span, "yt")
	span.SetTag("call_id", call.CallID.String())
	for _, field := range call.Params.Log() {
		if value, ok := field.Any().(fmt.Stringer); ok {
			span.SetTag(field.Key(), value.String())
		}
	}
	return
}

func (t *TracingInterceptor) traceFinish(span opentracing.Span, err error, tags ...opentracing.Tag) {
	if err != nil {
		ext.LogError(span, err)
	}
	for _, tag := range tags {
		span.SetTag(tag.Key, tag.Value)
	}
	span.Finish()
}

func (t *TracingInterceptor) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	span, ctx := t.traceStart(ctx, call)
	res, err = invoke(ctx, call)
	t.traceFinish(span, err)
	return
}

func (t *TracingInterceptor) Read(ctx context.Context, call *Call, invoke ReadInvoker) (r io.ReadCloser, err error) {
	span, ctx := t.traceStart(ctx, call)
	r, err = invoke(ctx, call)
	if err != nil {
		return
	}

	r = &tracingReader{span: span, t: t, r: r, call: call}
	return
}

func (t *TracingInterceptor) Write(ctx context.Context, call *Call, invoke WriteInvoker) (w io.WriteCloser, err error) {
	span, ctx := t.traceStart(ctx, call)
	w, err = invoke(ctx, call)
	if err != nil {
		return
	}

	lw := &tracingWriter{span: span, t: t, w: w, call: call}
	if call.RowBatch != nil {
		lw.byteCount = int64(call.RowBatch.Len())
	}

	w = lw
	return
}
