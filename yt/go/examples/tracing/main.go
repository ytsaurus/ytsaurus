package main

import (
	"context"
	"os"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ytotel"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"

	otelBridge "go.opentelemetry.io/otel/bridge/opentracing"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	ctx := context.Background()

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.Default()),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otelTracer := tp.Tracer("example-tracer")
	bridgeTracer := otelBridge.NewBridgeTracer()
	bridgeTracer.SetOpenTelemetryTracer(otelTracer)

	yc, _ := ytrpc.NewClient(&yt.Config{
		Proxy:   os.Getenv("YT_PROXY"),
		Tracer:  bridgeTracer,
		TraceFn: ytotel.TraceFn,
	})

	ctx, otelSpan := otelTracer.Start(ctx, "example-otel")
	defer otelSpan.End()

	ctx = bridgeTracer.ContextWithBridgeSpan(ctx, otelSpan)

	// Spans created by client will be linked to otel spans
	// Trace id and span id will be sent in requests to yt proxy 
	yc.ListNode(ctx, ypath.Path("/"), nil, nil)
}
