package solomon

import (
	"encoding/json"
	"time"

	"go.uber.org/atomic"

	"go.ytsaurus.tech/library/go/core/metrics"
)

var (
	_ metrics.IntGauge = (*IntGauge)(nil)
	_ Metric           = (*IntGauge)(nil)
)

// IntGauge tracks single int64 value.
type IntGauge struct {
	name       string
	metricType metricType
	tags       map[string]string
	value      atomic.Int64
	timestamp  *time.Time

	useNameTag bool
	memOnly    bool
}

func NewIntGauge(name string, value int64, opts ...MetricOpt) IntGauge {
	mOpts := MetricsOpts{}
	for _, op := range opts {
		op(&mOpts)
	}
	return IntGauge{
		name:       name,
		metricType: typeIGauge,
		tags:       mOpts.tags,
		value:      *atomic.NewInt64(value),
		timestamp:  mOpts.timestamp,

		useNameTag: mOpts.useNameTag,
		memOnly:    mOpts.memOnly,
	}
}

func (g *IntGauge) Set(value int64) {
	g.value.Store(value)
}

func (g *IntGauge) Add(value int64) {
	g.value.Add(value)
}

func (g *IntGauge) Name() string {
	return g.name
}

func (g *IntGauge) getType() metricType {
	return g.metricType
}

func (g *IntGauge) getLabels() map[string]string {
	return g.tags
}

func (g *IntGauge) getValue() interface{} {
	return g.value.Load()
}

func (g *IntGauge) getTimestamp() *time.Time {
	return g.timestamp
}

func (g *IntGauge) getNameTag() string {
	if g.useNameTag {
		return "name"
	} else {
		return "sensor"
	}
}

func (g *IntGauge) isMemOnly() bool {
	return g.memOnly
}

func (g *IntGauge) setMemOnly() {
	g.memOnly = true
}

// MarshalJSON implements json.Marshaler.
func (g *IntGauge) MarshalJSON() ([]byte, error) {
	metricType := g.metricType.String()
	value := g.value.Load()
	labels := func() map[string]string {
		labels := make(map[string]string, len(g.tags)+1)
		labels[g.getNameTag()] = g.Name()
		for k, v := range g.tags {
			labels[k] = v
		}
		return labels
	}()

	return json.Marshal(struct {
		Type      string            `json:"type"`
		Labels    map[string]string `json:"labels"`
		Value     int64             `json:"value"`
		Timestamp *int64            `json:"ts,omitempty"`
		MemOnly   bool              `json:"memOnly,omitempty"`
	}{
		Type:      metricType,
		Value:     value,
		Labels:    labels,
		Timestamp: tsAsRef(g.timestamp),
		MemOnly:   g.memOnly,
	})
}

// Snapshot returns independent copy of metric.
func (g *IntGauge) Snapshot() Metric {
	return &IntGauge{
		name:       g.name,
		metricType: g.metricType,
		tags:       g.tags,
		value:      *atomic.NewInt64(g.value.Load()),
		timestamp:  g.timestamp,

		useNameTag: g.useNameTag,
		memOnly:    g.memOnly,
	}
}
