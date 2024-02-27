package rpcclient

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"

	"go.ytsaurus.tech/yt/go/bus"
)

type TracingInterceptor struct {
	opentracing.Tracer
}

func (t *TracingInterceptor) traceStart(ctx context.Context, call *Call) (span opentracing.Span, retCtx context.Context) {
	span, retCtx = opentracing.StartSpanFromContextWithTracer(ctx, t.Tracer, string(call.Method), ext.SpanKindRPCClient)
	ext.Component.Set(span, "yt")
	span.SetTag("call_id", call.CallID.String())
	for _, field := range call.Req.Log() {
		if value, ok := field.Any().(fmt.Stringer); ok {
			span.SetTag(field.Key(), value.String())
		}
	}
	return
}

func (t *TracingInterceptor) traceFinish(span opentracing.Span, err error) {
	if err != nil {
		ext.LogError(span, err)
	}
	span.Finish()
}

func (t *TracingInterceptor) Intercept(ctx context.Context, call *Call, invoke CallInvoker, rsp proto.Message, opts ...bus.SendOption) (err error) {
	span, ctx := t.traceStart(ctx, call)
	err = invoke(ctx, call, rsp, opts...)
	t.traceFinish(span, err)
	return
}
