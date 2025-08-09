package main

import (
	"context"
	"fmt"
	"os"

	otelBridge "go.opentelemetry.io/otel/bridge/opentracing"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ytotel"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
)

func Example() error {
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
	var attrs map[string]any
	return yc.GetNode(ctx, ypath.Path("//@"), &attrs, nil)
}

func main() {
	if err := Example(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
}
