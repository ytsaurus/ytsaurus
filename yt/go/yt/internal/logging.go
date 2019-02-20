package internal

import (
	"context"
	"io"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/yt"
)

type LoggingInterceptor struct {
	log log.Logger
}

func (l *LoggingInterceptor) Intercept(ctx context.Context, call *Call, invoke CallInvoker) (res *CallResult, err error) {
	return invoke(ctx, call)
}

type loggingReader struct {
	r         io.ReadCloser
	call      *Call
	log       log.Logger
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

func (l *LoggingInterceptor) Read(ctx context.Context, call *Call, invoke ReadInvoker) (r io.ReadCloser, err error) {
	r, err = invoke(ctx, call)
	if err != nil {
		return
	}

	r = &loggingReader{r: r, call: call, log: l.log}
	return
}

type loggingWriter struct {
	w         io.WriteCloser
	call      *Call
	log       log.Logger
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

func (l *LoggingInterceptor) Write(ctx context.Context, call *Call, invoke WriteInvoker) (w io.WriteCloser, err error) {
	w, err = invoke(ctx, call)
	if err != nil {
		return
	}

	w = &loggingWriter{w: w, call: call, log: l.log}
	return
}

type loggingTableReader struct {
	r        yt.TableReader
	call     *Call
	log      log.Logger
	rowCount int64
}

func (l *LoggingInterceptor) ReadRow(ctx context.Context, call *Call, invoke ReadRowInvoker) (r yt.TableReader, err error) {
	return invoke(ctx, call)
}

type loggingTableWriter struct {
	r        yt.TableReader
	call     *Call
	log      log.Logger
	rowCount int64
}

func (l *LoggingInterceptor) WriteRow(ctx context.Context, call *Call, invoke WriteRowInvoker) (w yt.TableWriter, err error) {
	return invoke(ctx, call)
}
