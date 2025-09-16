package rpcclient

import (
	"context"

	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
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

func (l *LoggingInterceptor) Intercept(ctx context.Context, call *Call, invoke CallInvoker, rsp proto.Message, opts ...bus.SendOption) (err error) {
	ctx = l.logStart(ctx, call)
	err = invoke(ctx, call, rsp, opts...)
	var rspFields []log.Field
	if err == nil && rsp != nil {
		rspFields = responseLogFields(call, rsp)
	}
	l.logFinish(ctx, err, rspFields...)
	return
}

func responseLogFields(call *Call, rsp proto.Message) (fields []log.Field) {
	switch r := rsp.(type) {
	case *rpc_proxy.TRspCreateNode:
		fields = append(fields, log.Any("node_id", makeGUID(r.GetNodeId())))
	case *rpc_proxy.TRspCopyNode:
		fields = append(fields, log.Any("node_id", makeGUID(r.GetNodeId())))
	case *rpc_proxy.TRspMoveNode:
		fields = append(fields, log.Any("node_id", makeGUID(r.GetNodeId())))
	case *rpc_proxy.TRspLinkNode:
		fields = append(fields, log.Any("node_id", makeGUID(r.GetNodeId())))
	case *rpc_proxy.TRspStartTransaction:
		fields = append(fields, log.Any("transaction_id", makeGUID(r.GetId())))
	case *rpc_proxy.TRspStartOperation:
		fields = append(fields, log.Any("operation_id", makeGUID(r.GetOperationId())))
	case *rpc_proxy.TRspGenerateTimestamps:
		fields = append(fields, log.Any("timestamp", r.GetTimestamp()))
	}
	return
}
