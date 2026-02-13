package otel

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
)

type testMeterProvider struct {
	*sdkmetric.MeterProvider
	reader *sdkmetric.ManualReader
}

func newTestMeterProvider() *testMeterProvider {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	return &testMeterProvider{
		MeterProvider: mp,
		reader:        reader,
	}
}

func (p *testMeterProvider) Collect() (metricdata.ResourceMetrics, error) {
	var data metricdata.ResourceMetrics
	err := p.reader.Collect(context.Background(), &data)
	return data, err
}

func (p *testMeterProvider) AssertMetric(t *testing.T, expected, actual metricdata.Metrics, opts ...metricdatatest.Option) bool {
	return metricdatatest.AssertEqual(t, expected, actual, opts...)
}
