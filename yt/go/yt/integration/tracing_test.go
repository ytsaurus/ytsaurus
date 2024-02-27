package integration

import (
	"net/http/httptrace"
	"net/textproto"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ytjaeger"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestTracing(t *testing.T) {
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

	env := yttest.New(t, yttest.WithConfig(yt.Config{
		TraceFn: ytjaeger.TraceFn,
	}))

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

	var attrs any
	require.NoError(t, env.YT.GetNode(ctx, ypath.Path("//@"), &attrs, nil))
	require.NotEmpty(t, traceParent)

	require.NoError(t, env.YT.GetNode(ctx, ypath.Path("//@"), &attrs, nil))

	// Indirect smoke test for tracingReader/tracingWriter
	name := tmpPath()
	_, err = env.YT.CreateNode(ctx, name, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"schema": schema.MustInfer(&exampleRow{}),
		},
	})
	require.NoError(t, err)

	w, err := env.YT.WriteTable(ctx, name, nil)
	require.NoError(t, err)
	require.NoError(t, w.Write(exampleRow{"foo", 1}))
	require.NoError(t, w.Commit())

	r, err := env.YT.ReadTable(ctx, name, nil)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	var s exampleRow
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&s))
	require.Equal(t, exampleRow{"foo", 1}, s)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}
