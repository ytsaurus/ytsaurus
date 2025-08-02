package ytotel

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"

	otelBridge "go.opentelemetry.io/otel/bridge/opentracing"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestTraceFn(t *testing.T) {
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.Default()),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otelTracer := tp.Tracer("test-tracer")
	bridgeTracer := otelBridge.NewBridgeTracer()
	bridgeTracer.SetOpenTelemetryTracer(otelTracer)

	otSpan, ctx := opentracing.StartSpanFromContextWithTracer(context.Background(), bridgeTracer, "test_tracing")
	defer otSpan.Finish()

	traceID, spanID, flags, ok := TraceFn(ctx)
	assert.True(t, ok)
	assert.NotEmpty(t, traceID)
	assert.NotZero(t, spanID)
	assert.Equal(t, byte(0x01), flags)
	assert.Len(t, traceID, 16, "trace ID should be 16 bytes")
	assert.NotZero(t, spanID, "span ID should be non-zero")
}
