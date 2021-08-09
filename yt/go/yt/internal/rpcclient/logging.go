package rpcclient

import (
	"context"

	"github.com/golang/protobuf/proto"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/ctxlog"
)

type LoggingInterceptor struct {
	log.Structured
}

func logFields(call *Call) (fields []log.Field) {
	fields = []log.Field{
		log.String("method", string(call.Method)),
	}
	fields = append(fields, call.Req.Log()...)
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

func (l *LoggingInterceptor) Intercept(ctx context.Context, call *Call, invoke CallInvoker, rsp proto.Message) (err error) {
	ctx = l.logStart(ctx, call)
	err = invoke(ctx, call, rsp)
	l.logFinish(ctx, err)
	return
}
