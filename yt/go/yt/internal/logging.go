package internal

import (
	"context"
	"io"

	"go.uber.org/atomic"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
)

type loggingReader struct {
	ctx       context.Context
	l         *LoggingInterceptor
	r         io.ReadCloser
	call      *Call
	byteCount int64

	logged atomic.Bool
}

func (r *loggingReader) logFinish(err error, fields ...log.Field) {
	if !r.logged.Swap(true) {
		r.l.logFinish(r.ctx, err, fields...)
	}
}

func (r *loggingReader) Close() error {
	if !r.logged.Swap(true) {
		ctxlog.Error(r.ctx, r.l.Logger(), "request interrupted", log.Int64("bytes_read", r.byteCount))
	}

	return r.r.Close()
}

func (r *loggingReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	r.byteCount += int64(n)

	if err != nil {
		if err == io.EOF {
			r.logFinish(nil, log.Int64("bytes_read", r.byteCount))
		} else {
			r.logFinish(err, log.Int64("bytes_read", r.byteCount))
		}
	}

	return
}

type loggingWriter struct {
	ctx       context.Context
	l         *LoggingInterceptor
	w         io.WriteCloser
	call      *Call
	byteCount int64

	logged atomic.Bool
}

func (w *loggingWriter) logFinish(err error, fields ...log.Field) {
	if !w.logged.Swap(true) {
		w.l.logFinish(w.ctx, err, fields...)
	}
}

func (w *loggingWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.byteCount += int64(n)

	if err != nil {
		w.logFinish(err, log.Int64("bytes_written", w.byteCount))
	}

	return
}

func (w *loggingWriter) Close() error {
	err := w.w.Close()
	w.logFinish(err, log.Int64("bytes_written", w.byteCount))
	return err
}

type LoggingInterceptor struct {
	log.Structured
}

func logFields(call *Call) (fields []log.Field) {
	fields = []log.Field{
		log.String("method", call.Params.HTTPVerb().String()),
	}
	fields = append(fields, call.Params.Log()...)

	return
}

func (l *LoggingInterceptor) logStart(ctx context.Context, call *Call) context.Context {
	ctx = ctxlog.WithFields(ctx, log.String("call_id", call.CallID.String()))
	ctxlog.Debug(ctx, l.Logger(), "request started", logFields(call)...)
	return ctx
}

func (l *LoggingInterceptor) logFinish(ctx context.Context, err error, fields ...log.Field) {
	if err != nil {
		fields = append(fields, log.Error(err))
		ctxlog.Error(ctx, l.Logger(), "request failed", fields...)
	} else {
		ctxlog.Debug(ctx, l.Logger(), "request finished", fields...)
	}
}

func (l *LoggingInterceptor) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	ctx = l.logStart(ctx, call)
	res, err = invoke(ctx, call)
	l.logFinish(ctx, err)
	return
}

func (l *LoggingInterceptor) Read(ctx context.Context, call *Call, invoke ReadInvoker) (r io.ReadCloser, err error) {
	ctx = l.logStart(ctx, call)
	r, err = invoke(ctx, call)
	if err != nil {
		return
	}

	r = &loggingReader{ctx: ctx, l: l, r: r, call: call}
	return
}

func (l *LoggingInterceptor) Write(ctx context.Context, call *Call, invoke WriteInvoker) (w io.WriteCloser, err error) {
	ctx = l.logStart(ctx, call)
	w, err = invoke(ctx, call)
	if err != nil {
		return
	}

	lw := &loggingWriter{ctx: ctx, l: l, w: w, call: call}
	if call.RowBatch != nil {
		lw.byteCount = int64(call.RowBatch.Len())
	}

	w = lw
	return
}
