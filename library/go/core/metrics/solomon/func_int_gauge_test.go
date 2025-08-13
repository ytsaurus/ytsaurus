package solomon

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestFuncIntGauge_Value(t *testing.T) {
	val := new(atomic.Int64)
	c := &FuncIntGauge{
		name:       "myintgauge",
		metricType: typeIGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return val.Load()
		},
	}

	val.Store(1)
	assert.Equal(t, int64(1), c.Snapshot().(*IntGauge).value.Load())

	val.Store(42)
	assert.Equal(t, int64(42), c.Snapshot().(*IntGauge).value.Load())
}

func TestFuncIntGauge_getID(t *testing.T) {
	val := new(atomic.Int64)
	c := &FuncIntGauge{
		name:       "myintgauge",
		metricType: typeIGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return val.Load()
		},
	}

	assert.Equal(t, "myintgauge", c.getID())
}

func TestFuncIntGauge_getID_WithTS(t *testing.T) {
	ts := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	val := new(atomic.Int64)
	c := &FuncIntGauge{
		name:       "myintgauge",
		metricType: typeIGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return val.Load()
		},
		timestamp: &ts,
	}

	assert.Equal(t, "myintgauge(2020-01-01T00:00:00Z)", c.getID())
}

func TestFuncIntGauge_MarshalJSON(t *testing.T) {
	ts := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	c := &FuncIntGauge{
		name:       "myintgauge",
		metricType: typeIGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return 42
		},
		timestamp: &ts,
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"IGAUGE","labels":{"ololo":"trololo","sensor":"myintgauge"},"value":42,"ts":1577836800}`)
	assert.Equal(t, expected, b)
}

func TestNameTagFunIntGauge_MarshalJSON(t *testing.T) {
	c := &FuncIntGauge{
		name:       "myintgauge",
		metricType: typeIGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return 42
		},

		useNameTag: true,
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"IGAUGE","labels":{"name":"myintgauge","ololo":"trololo"},"value":42}`)
	assert.Equal(t, expected, b)
}
