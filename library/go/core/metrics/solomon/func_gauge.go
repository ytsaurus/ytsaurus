package solomon

import (
	"encoding/json"
	"time"

	"go.uber.org/atomic"
)

var _ Metric = (*FuncGauge)(nil)

// FuncGauge tracks float64 value returned by function.
type FuncGauge struct {
	name       string
	metricType metricType
	tags       map[string]string
	function   func() float64
	timestamp  *time.Time

	useNameTag bool
	memOnly    bool
}

func (g *FuncGauge) Name() string {
	return g.name
}

func (g *FuncGauge) Function() func() float64 {
	return g.function
}

func (g *FuncGauge) getType() metricType {
	return g.metricType
}

func (g *FuncGauge) getLabels() map[string]string {
	return g.tags
}

func (g *FuncGauge) getValue() interface{} {
	return g.function()
}

func (g *FuncGauge) getTimestamp() *time.Time {
	return g.timestamp
}

func (g *FuncGauge) getNameTag() string {
	if g.useNameTag {
		return "name"
	} else {
		return "sensor"
	}
}

func (g *FuncGauge) isMemOnly() bool {
	return g.memOnly
}

func (g *FuncGauge) setMemOnly() {
	g.memOnly = true
}

// MarshalJSON implements json.Marshaler.
func (g *FuncGauge) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string            `json:"type"`
		Labels    map[string]string `json:"labels"`
		Value     float64           `json:"value"`
		Timestamp *int64            `json:"ts,omitempty"`
		MemOnly   bool              `json:"memOnly,omitempty"`
	}{
		Type:  g.metricType.String(),
		Value: g.function(),
		Labels: func() map[string]string {
			labels := make(map[string]string, len(g.tags)+1)
			labels[g.getNameTag()] = g.Name()
			for k, v := range g.tags {
				labels[k] = v
			}
			return labels
		}(),
		Timestamp: tsAsRef(g.timestamp),
		MemOnly:   g.memOnly,
	})
}

// Snapshot returns independent copy on metric.
func (g *FuncGauge) Snapshot() Metric {
	return &Gauge{
		name:       g.name,
		metricType: g.metricType,
		tags:       g.tags,
		value:      *atomic.NewFloat64(g.function()),

		useNameTag: g.useNameTag,
		memOnly:    g.memOnly,
	}
}
