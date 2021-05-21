package ytjaeger

import (
	"context"

	"a.yandex-team.ru/yt/go/guid"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
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
