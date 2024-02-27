package ytjaeger

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"

	"go.ytsaurus.tech/yt/go/guid"
)

func TraceFn(ctx context.Context) (traceID guid.GUID, spanID uint64, flags byte, ok bool) {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return
	}

	jaegerSC, ok := span.Context().(jaeger.SpanContext)
	if !ok {
		return
	}

	traceID = guid.FromHalves(jaegerSC.TraceID().Low, jaegerSC.TraceID().High)
	spanID = uint64(jaegerSC.SpanID())
	flags = jaegerSC.Flags()
	ok = true
	return
}
