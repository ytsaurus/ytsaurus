package otel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"go.ytsaurus.tech/library/go/core/metrics"
)

func TestRegistry_CounterVec(t *testing.T) {
	mp := newTestMeterProvider()

	tags := map[string]string{"ololo": "trololo"}
	r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

	vec := r.CounterVec("test_counter_vec", []string{"shimba", "looken"})
	vectags := map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	}
	metric := vec.With(vectags)
	metric2 := vec.With(vectags)

	assert.Same(t, metric, metric2)

	metric.Add(42)

	data, err := mp.Collect()
	require.NoError(t, err)
	require.NotEmpty(t, data.ScopeMetrics)
	require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

	expected := metricdata.Metrics{
		Name: "test_counter_vec",
		Data: metricdata.Sum[int64]{
			DataPoints: []metricdata.DataPoint[int64]{
				{
					Value: 42,
					Attributes: tagsToAttributes(map[string]string{
						"ololo":  "trololo",
						"shimba": "boomba",
						"looken": "tooken",
					}),
				},
			},
			Temporality: metricdata.CumulativeTemporality,
			IsMonotonic: true,
		},
	}

	metricdatatest.AssertEqual(t,
		expected,
		data.ScopeMetrics[0].Metrics[0],
		metricdatatest.IgnoreTimestamp(),
	)
}

func TestRegistry_GaugeVec(t *testing.T) {
	mp := newTestMeterProvider()

	tags := map[string]string{"ololo": "trololo"}
	r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

	vec := r.GaugeVec("test_gauge_vec", []string{"shimba", "looken"})
	vectags := map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	}
	metric := vec.With(vectags)
	metric2 := vec.With(vectags)

	assert.Same(t, metric, metric2)

	metric.Add(42)

	data, err := mp.Collect()
	require.NoError(t, err)
	require.NotEmpty(t, data.ScopeMetrics)
	require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

	expected := metricdata.Metrics{
		Name: "test_gauge_vec",
		Data: metricdata.Gauge[float64]{
			DataPoints: []metricdata.DataPoint[float64]{
				{
					Value: 42,
					Attributes: tagsToAttributes(map[string]string{
						"ololo":  "trololo",
						"shimba": "boomba",
						"looken": "tooken",
					}),
				},
			},
		},
	}

	metricdatatest.AssertEqual(t,
		expected,
		data.ScopeMetrics[0].Metrics[0],
		metricdatatest.IgnoreTimestamp(),
	)
}

func TestRegistry_IntGaugeVec(t *testing.T) {
	mp := newTestMeterProvider()

	tags := map[string]string{"ololo": "trololo"}
	r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

	vec := r.IntGaugeVec("test_int_gauge_vec", []string{"shimba", "looken"})
	vectags := map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	}
	metric := vec.With(vectags)
	metric2 := vec.With(vectags)

	assert.Same(t, metric, metric2)

	metric.Add(42)

	data, err := mp.Collect()
	require.NoError(t, err)
	require.NotEmpty(t, data.ScopeMetrics)
	require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

	expected := metricdata.Metrics{
		Name: "test_int_gauge_vec",
		Data: metricdata.Gauge[int64]{
			DataPoints: []metricdata.DataPoint[int64]{
				{
					Value: 42,
					Attributes: tagsToAttributes(map[string]string{
						"ololo":  "trololo",
						"shimba": "boomba",
						"looken": "tooken",
					}),
				},
			},
		},
	}

	metricdatatest.AssertEqual(t,
		expected,
		data.ScopeMetrics[0].Metrics[0],
		metricdatatest.IgnoreTimestamp(),
	)
}

func TestRegistry_TimerVec(t *testing.T) {
	mp := newTestMeterProvider()

	tags := map[string]string{"ololo": "trololo"}
	r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

	vec := r.TimerVec("test_timer_vec", []string{"shimba", "looken"})
	vectags := map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	}
	metric := vec.With(vectags)
	metric2 := vec.With(vectags)

	assert.Same(t, metric, metric2)

	metric.RecordDuration(42 * time.Millisecond)

	data, err := mp.Collect()
	require.NoError(t, err)
	require.NotEmpty(t, data.ScopeMetrics)
	require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

	expected := metricdata.Metrics{
		Name: "test_timer_vec",
		Data: metricdata.Gauge[int64]{
			DataPoints: []metricdata.DataPoint[int64]{
				{
					Value: int64(42 * time.Millisecond),
					Attributes: tagsToAttributes(map[string]string{
						"ololo":  "trololo",
						"shimba": "boomba",
						"looken": "tooken",
					}),
				},
			},
		},
	}

	metricdatatest.AssertEqual(t,
		expected,
		data.ScopeMetrics[0].Metrics[0],
		metricdatatest.IgnoreTimestamp(),
	)
}

func TestRegistry_HistogramVec(t *testing.T) {
	mp := newTestMeterProvider()

	tags := map[string]string{"ololo": "trololo"}
	r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

	buckets := metrics.MakeLinearBuckets(0, 42, 4)
	vec := r.HistogramVec("test_histogram_vec", buckets, []string{"shimba", "looken"})
	vectags := map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	}

	metric := vec.With(vectags)
	metric2 := vec.With(vectags)

	assert.Same(t, metric, metric2)

	metric.RecordValue(69)

	data, err := mp.Collect()
	require.NoError(t, err)
	require.NotEmpty(t, data.ScopeMetrics)
	require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

	expected := metricdata.Metrics{
		Name: "test_histogram_vec",
		Data: metricdata.Histogram[float64]{
			DataPoints: []metricdata.HistogramDataPoint[float64]{
				{
					Attributes: tagsToAttributes(map[string]string{
						"ololo":  "trololo",
						"shimba": "boomba",
						"looken": "tooken",
					}),
					Count:        1,
					Bounds:       []float64{0, 42, 84, 126},
					BucketCounts: []uint64{0, 0, 1, 0, 0},
					Min:          metricdata.NewExtrema(69.0),
					Max:          metricdata.NewExtrema(69.0),
					Sum:          69,
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
}

func TestRegistry_DurationHistogramVec(t *testing.T) {
	mp := newTestMeterProvider()

	tags := map[string]string{"ololo": "trololo"}
	r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

	buckets := metrics.NewDurationBuckets(0, 42*time.Millisecond, 4*time.Second)
	vec := r.DurationHistogramVec("test_duration_histogram_vec", buckets, []string{"shimba", "looken"})
	vectags := map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	}

	metric := vec.With(vectags)
	metric2 := vec.With(vectags)

	assert.Same(t, metric, metric2)

	metric.RecordDuration(69 * time.Millisecond)

	data, err := mp.Collect()
	require.NoError(t, err)
	require.NotEmpty(t, data.ScopeMetrics)
	require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

	expected := metricdata.Metrics{
		Name: "test_duration_histogram_vec",
		Data: metricdata.Histogram[float64]{
			DataPoints: []metricdata.HistogramDataPoint[float64]{
				{
					Attributes: tagsToAttributes(map[string]string{
						"ololo":  "trololo",
						"shimba": "boomba",
						"looken": "tooken",
					}),
					Count:        1,
					Bounds:       []float64{0, 0.042, 4},
					BucketCounts: []uint64{0, 0, 1, 0},
					Min:          metricdata.NewExtrema(0.069),
					Max:          metricdata.NewExtrema(0.069),
					Sum:          0.069,
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
}
