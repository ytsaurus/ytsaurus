package solomon

import "time"

type MetricsOpts struct {
	useNameTag bool
	tags       map[string]string
	timestamp  *time.Time
	memOnly    bool
	rated      bool
}

type MetricOpt func(*MetricsOpts)

func WithTags(tags map[string]string) func(*MetricsOpts) {
	return func(m *MetricsOpts) {
		m.tags = tags
	}
}

func WithUseNameTag() func(*MetricsOpts) {
	return func(m *MetricsOpts) {
		m.useNameTag = true
	}
}

func WithTimestamp(t time.Time) func(*MetricsOpts) {
	return func(m *MetricsOpts) {
		m.timestamp = &t
	}
}

func WithMemOnly() func(*MetricsOpts) {
	return func(m *MetricsOpts) {
		m.memOnly = true
	}
}

func WithRated(rated bool) func(*MetricsOpts) {
	return func(m *MetricsOpts) {
		m.rated = rated
	}
}
