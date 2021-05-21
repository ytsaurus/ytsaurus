package integration

import (
	"net/http/httptrace"
	"net/textproto"
	"testing"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ytjaeger"
	"a.yandex-team.ru/yt/go/yttest"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

func TestTracing(t *testing.T) {
	env := yttest.New(t, yttest.WithConfig(yt.Config{
		TraceFn: ytjaeger.TraceFn,
	}))

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
	}.NewTracer()
	require.NoError(t, err)

	defer closer.Close()
	opentracing.SetGlobalTracer(tracer)
	defer func() {
		opentracing.SetGlobalTracer(opentracing.NoopTracer{})
	}()

	var traceParent []string
	clientTrace := &httptrace.ClientTrace{
		WroteHeaderField: func(key string, value []string) {
			t.Logf("header %s value is %v", key, value)

			if textproto.CanonicalMIMEHeaderKey(key) == textproto.CanonicalMIMEHeaderKey("traceparent") {
				traceParent = value
			}
		},
	}

	ctx := httptrace.WithClientTrace(env.Ctx, clientTrace)

	span, ctx := opentracing.StartSpanFromContext(ctx, "test_tracing")
	defer span.Finish()

	var attrs interface{}
	require.NoError(t, env.YT.GetNode(ctx, ypath.Path("//@"), &attrs, nil))
	require.NotEmpty(t, traceParent)

	require.NoError(t, env.YT.GetNode(ctx, ypath.Path("//@"), &attrs, nil))
}
