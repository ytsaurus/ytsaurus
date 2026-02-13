package otel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"go.ytsaurus.tech/library/go/core/metrics"
)

func TestRegistry_Histogram(t *testing.T) {
	t.Run("record_value", func(t *testing.T) {
		mp := newTestMeterProvider()

		tags := map[string]string{"ololo": "trololo"}
		r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

		buckets := metrics.MakeLinearBuckets(0, 42, 4)
		hist := r.Histogram("test_histogram", buckets)

		hist.RecordValue(42)
		hist.RecordValue(42)
		hist.RecordValue(96)
		hist.RecordValue(128)

		data, err := mp.Collect()
		require.NoError(t, err)
		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_histogram",
			Data: metricdata.Histogram[float64]{
				DataPoints: []metricdata.HistogramDataPoint[float64]{
					{
						Attributes:   tagsToAttributes(tags),
						Count:        4,
						Bounds:       []float64{0, 42, 84, 126},
						BucketCounts: []uint64{0, 2, 0, 1, 1},
						Min:          metricdata.NewExtrema(42.0),
						Max:          metricdata.NewExtrema(128.0),
						Sum:          308,
					},
				},
				Temporality: metricdata.CumulativeTemporality,
			},
		}

		metricdatatest.AssertEqual(t,
			expected,
			data.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	})
}

func TestRegistry_DurationHistogram(t *testing.T) {
	t.Run("record_duration", func(t *testing.T) {
		mp := newTestMeterProvider()

		tags := map[string]string{"ololo": "trololo"}
		r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

		buckets := metrics.NewDurationBuckets(0, 42*time.Millisecond, time.Second)
		hist := r.DurationHistogram("test_duration_histogram", buckets)

		hist.RecordDuration(100 * time.Nanosecond)
		hist.RecordDuration(100 * time.Millisecond)
		hist.RecordDuration(2 * time.Second)

		data, err := mp.Collect()
		require.NoError(t, err)
		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_duration_histogram",
			Data: metricdata.Histogram[float64]{
				DataPoints: []metricdata.HistogramDataPoint[float64]{
					{
						Attributes:   tagsToAttributes(tags),
						Count:        3,
						Bounds:       []float64{0, 0.042, 1},
						BucketCounts: []uint64{0, 1, 1, 1},
						Min:          metricdata.NewExtrema(0.0000001),
						Max:          metricdata.NewExtrema(2.0),
						Sum:          2.1000001,
					},
				},
				Temporality: metricdata.CumulativeTemporality,
			},
		}

		metricdatatest.AssertEqual(t,
			expected,
			data.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	})
}
