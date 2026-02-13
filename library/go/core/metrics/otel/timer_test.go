package otel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
)

func TestRegistry_Timer(t *testing.T) {
	t.Run("record", func(t *testing.T) {
		mp := newTestMeterProvider()

		tags := map[string]string{"ololo": "trololo"}
		r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

		r.Timer("test_timer").RecordDuration(42 * time.Second)

		data, err := mp.Collect()
		require.NoError(t, err)
		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_timer",
			Data: metricdata.Gauge[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value:      int64(42 * time.Second),
						Attributes: tagsToAttributes(tags),
					},
				},
			},
		}

		metricdatatest.AssertEqual(t,
			expected,
			data.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	})
}
