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

func TestRegistry_Counter(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		mp := newTestMeterProvider()

		tags := map[string]string{"ololo": "trololo"}
		r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

		r.Counter("test_counter").Add(42)

		data, err := mp.Collect()
		require.NoError(t, err)
		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_counter",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value:      42,
						Attributes: tagsToAttributes(tags),
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
	})

	t.Run("inc", func(t *testing.T) {
		mp := newTestMeterProvider()

		tags := map[string]string{"ololo": "trololo"}
		r := NewRegistry("test", WithMeterProvider(mp)).WithTags(tags)

		cnt := r.Counter("test_counter")
		cnt.Add(42)
		cnt.Inc()

		data, err := mp.Collect()
		require.NoError(t, err)
		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_counter",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value:      43,
						Attributes: tagsToAttributes(tags),
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
	})
}

func TestRegistry_FuncCounter(t *testing.T) {
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
	fc := r.FuncCounter("test_func_counter", fn)

	// check if function returned by counter is the same as passed to constructor
	assert.Equal(t, fmt.Sprintf("%p", fn), fmt.Sprintf("%p", fc.Function()))

	// first collection cycle
	{
		data, err := mp.Collect()
		require.NoError(t, err)

		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_func_counter",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value:      42,
						Attributes: tagsToAttributes(tags),
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

	// second collection cycle
	{
		data, err := mp.Collect()
		require.NoError(t, err)

		require.NotEmpty(t, data.ScopeMetrics)
		require.NotEmpty(t, data.ScopeMetrics[0].Metrics)

		expected := metricdata.Metrics{
			Name: "test_func_counter",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value:      69,
						Attributes: tagsToAttributes(tags),
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
}
