package internal

import (
	"context"
	"io"

	"a.yandex-team.ru/library/go/core/log"
)

type loggingReader struct {
	r         io.ReadCloser
	call      *Call
	log       log.Structured
	byteCount int64
}

func (r *loggingReader) Close() error {
	return r.r.Close()
}

func (r *loggingReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	r.byteCount += int64(n)

	return
}

type loggingWriter struct {
	w         io.WriteCloser
	call      *Call
	log       log.Structured
	byteCount int64
}

func (w *loggingWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.byteCount += int64(n)
	return
}

func (w *loggingWriter) Close() error {
	return w.w.Close()
}

type LoggingInterceptor struct {
	log.Structured
}

func logFields(call *Call) (fields []log.Field) {
	fields = []log.Field{
		log.String("method", call.Params.HTTPVerb().String()),
		log.String("call_id", call.CallID.String()),
	}
	fields = append(fields, call.Params.Log()...)
	return
}

func (l *LoggingInterceptor) logStart(call *Call) {
	l.Debug("request started", logFields(call)...)
}

func (l *LoggingInterceptor) logFinish(call *Call, err error) {
	fields := logFields(call)
	if err != nil {
		fields = append(fields, log.Error(err))
		l.Error("request failed", fields...)
	} else {
		l.Debug("request finished", fields...)
	}
}

func (l *LoggingInterceptor) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	l.logStart(call)
	res, err = invoke(ctx, call)
	l.logFinish(call, err)
	return
}

func (l *LoggingInterceptor) Read(ctx context.Context, call *Call, invoke ReadInvoker) (r io.ReadCloser, err error) {
	l.logStart(call)
	r, err = invoke(ctx, call)
	if err != nil {
		return
	}

	r = &loggingReader{r: r, call: call, log: l.Structured}
	return
}

func (l *LoggingInterceptor) Write(ctx context.Context, call *Call, invoke WriteInvoker) (w io.WriteCloser, err error) {
	l.logStart(call)
	w, err = invoke(ctx, call)
	if err != nil {
		return
	}

	w = &loggingWriter{w: w, call: call, log: l.Structured}
	return
}
