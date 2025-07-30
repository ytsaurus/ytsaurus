package ytotel

import (
	"context"
	"encoding/binary"
	"github.com/opentracing/opentracing-go"
	"go.opentelemetry.io/otel/trace"
	"go.ytsaurus.tech/yt/go/guid"
)

func TraceFn(ctx context.Context) (traceID guid.GUID, spanID uint64, flags byte, ok bool) {
	if otSpan := opentracing.SpanFromContext(ctx); otSpan != nil {
		spanCtx := otSpan.Context()

		// Try to get OpenTelemetry context from the OpenTracing span via bridge
		// The bridge should implement these methods
		type spanContextProvider interface {
			TraceID() trace.TraceID
			SpanID() trace.SpanID
			TraceFlags() trace.TraceFlags
		}

		if bridgeSpanCtx, ok := spanCtx.(spanContextProvider); ok {
			traceIDBytes := bridgeSpanCtx.TraceID()
			spanIDBytes := bridgeSpanCtx.SpanID()

			if len(traceIDBytes) == 16 {
				high := binary.BigEndian.Uint64(traceIDBytes[:8])
				low := binary.BigEndian.Uint64(traceIDBytes[8:])
				traceID = guid.FromHalves(low, high)
			}

			if len(spanIDBytes) == 8 {
				spanID = binary.BigEndian.Uint64(spanIDBytes[:])
			}

			flags := byte(bridgeSpanCtx.TraceFlags())

			return traceID, spanID, flags, true
		}
	}

	return
}
