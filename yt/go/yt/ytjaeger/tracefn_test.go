package ytjaeger

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

func TestTraceFn(t *testing.T) {
	tracer, closer, err := config.Configuration{
		ServiceName: "debug",
		Sampler: &config.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			CollectorEndpoint: "localhost:12345",
			LogSpans:          true,
		},
	}.NewTracer(config.Gen128Bit(true))
	require.NoError(t, err)

	defer closer.Close()
	opentracing.SetGlobalTracer(tracer)
	defer func() {
		opentracing.SetGlobalTracer(opentracing.NoopTracer{})
	}()

	span, ctx := opentracing.StartSpanFromContext(context.Background(), "test_tracing")
	defer span.Finish()

	traceID, spanID, flags, ok := TraceFn(ctx)
	assert.True(t, ok)

	fix := func(n int, s string) string {
		return strings.Repeat("0", n-len(s)) + s
	}

	jsc := span.Context().(jaeger.SpanContext)
	assert.Equal(t, traceID.HexString(), fix(32, jsc.TraceID().String()))
	assert.Equal(t, fmt.Sprintf("%016x", spanID), fix(16, jsc.SpanID().String()))

	assert.Equal(t, byte(0x01), flags)
}
