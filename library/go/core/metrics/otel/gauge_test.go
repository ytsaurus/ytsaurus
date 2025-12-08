package otel

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
)

func TestRegistry_Gauge(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		mp := newTestMeterProvider()

		tags := map[string]string{"ololo": "trololo"}
		r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

		r.Gauge("test_gauge").Set(42)

		data, err := mp.Collect()
		require.NoError(t, err)
		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_gauge",
			Data: metricdata.Gauge[float64]{
				DataPoints: []metricdata.DataPoint[float64]{
					{Value: 42, Attributes: tagsToAttributes(tags)},
				},
			},
		}

		metricdatatest.AssertEqual(t,
			expected,
			data.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	})

	t.Run("add", func(t *testing.T) {
		mp := newTestMeterProvider()

		tags := map[string]string{"ololo": "trololo"}
		r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

		gg := r.Gauge("test_gauge")
		gg.Set(42)
		gg.Add(1)

		data, err := mp.Collect()
		require.NoError(t, err)
		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_gauge",
			Data: metricdata.Gauge[float64]{
				DataPoints: []metricdata.DataPoint[float64]{
					{Value: 43, Attributes: tagsToAttributes(tags)},
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

func TestRegistry_IntGauge(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		mp := newTestMeterProvider()

		tags := map[string]string{"ololo": "trololo"}
		r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

		r.IntGauge("test_int_gauge").Set(42)

		data, err := mp.Collect()
		require.NoError(t, err)
		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_int_gauge",
			Data: metricdata.Gauge[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 42, Attributes: tagsToAttributes(tags)},
				},
			},
		}

		metricdatatest.AssertEqual(t,
			expected,
			data.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	})

	t.Run("add", func(t *testing.T) {
		mp := newTestMeterProvider()

		tags := map[string]string{"ololo": "trololo"}
		r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

		gg := r.IntGauge("test_int_gauge")
		gg.Set(42)
		gg.Add(1)

		data, err := mp.Collect()
		require.NoError(t, err)
		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_int_gauge",
			Data: metricdata.Gauge[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 43, Attributes: tagsToAttributes(tags)},
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

func TestRegistry_FuncGauge(t *testing.T) {
	mp := newTestMeterProvider()

	var calls uint32
	fn := func() float64 {
		defer atomic.AddUint32(&calls, 1)
		if atomic.LoadUint32(&calls)%2 == 0 {
			return 42
		}
		return 69
	}

	tags := map[string]string{"ololo": "trololo"}
	r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)
	fc := r.FuncGauge("test_func_gauge", fn)

	// check if function returned by counter is the same as passed to constructor
	assert.Equal(t, fmt.Sprintf("%p", fn), fmt.Sprintf("%p", fc.Function()))

	// first collection cycle
	{
		data, err := mp.Collect()
		require.NoError(t, err)

		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_func_gauge",
			Data: metricdata.Gauge[float64]{
				DataPoints: []metricdata.DataPoint[float64]{
					{Value: 42, Attributes: tagsToAttributes(tags)},
				},
			},
		}

		metricdatatest.AssertEqual(t,
			expected,
			data.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	}

	// second collection cycle
	{
		data, err := mp.Collect()
		require.NoError(t, err)

		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_func_gauge",
			Data: metricdata.Gauge[float64]{
				DataPoints: []metricdata.DataPoint[float64]{
					{Value: 69, Attributes: tagsToAttributes(tags)},
				},
			},
		}

		metricdatatest.AssertEqual(t,
			expected,
			data.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	}
}

func TestRegistry_FuncIntGauge(t *testing.T) {
	mp := newTestMeterProvider()

	var calls uint32
	fn := func() int64 {
		defer atomic.AddUint32(&calls, 1)
		if atomic.LoadUint32(&calls)%2 == 0 {
			return 42
		}
		return 69
	}

	tags := map[string]string{"ololo": "trololo"}
	r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)
	fc := r.FuncIntGauge("test_func_int_gauge", fn)

	// check if function returned by counter is the same as passed to constructor
	assert.Equal(t, fmt.Sprintf("%p", fn), fmt.Sprintf("%p", fc.Function()))

	// first collection cycle
	{
		data, err := mp.Collect()
		require.NoError(t, err)

		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_func_int_gauge",
			Data: metricdata.Gauge[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 42, Attributes: tagsToAttributes(tags)},
				},
			},
		}

		metricdatatest.AssertEqual(t,
			expected,
			data.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	}

	// second collection cycle
	{
		data, err := mp.Collect()
		require.NoError(t, err)

		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_func_int_gauge",
			Data: metricdata.Gauge[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 69, Attributes: tagsToAttributes(tags)},
				},
			},
		}

		metricdatatest.AssertEqual(t,
			expected,
			data.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	}
}
