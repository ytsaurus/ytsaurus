package timbertruck

import (
	"log/slog"

	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
)

type activeTaskCounter struct {
	logger    *slog.Logger
	datastore *Datastore
	counter   map[string]int
	metrics   map[string]metrics.IntGauge
}

func newActiveTaskCounter(logger *slog.Logger, streamNames []string, registry metrics.Registry, datastore *Datastore) *activeTaskCounter {
	result := &activeTaskCounter{
		logger:    logger,
		datastore: datastore,
		counter:   make(map[string]int),
		metrics:   make(map[string]metrics.IntGauge),
	}

	for _, streamName := range streamNames {
		result.counter[streamName] = 0
		result.metrics[streamName] = registry.WithTags(map[string]string{"stream": streamName}).IntGauge("tt.stream.active_tasks")
	}

	return result
}

func (c *activeTaskCounter) Do() {
	tasks, err := c.datastore.ListActiveTasks()
	if err != nil {
		c.logger.Warn("Unexpected error ListActiveTasks()", "error", err)
		return
	}

	for name := range c.counter {
		c.counter[name] = 0
	}

	for i := range tasks {
		c.counter[tasks[i].StreamName]++
	}

	for name, count := range c.counter {
		intGauge, ok := c.metrics[name]
		// There might be active tasks from old configuration.
		// We account only active configuration.
		if ok {
			intGauge.Set(int64(count))
		}
	}
}

type skippedRowsMetrics interface {
	Inc(reason pipelines.SkipRowReason)
}

type skippedRowsMetricsImpl struct {
	logger   *slog.Logger
	counters map[pipelines.SkipRowReason]metrics.Counter
}

func (m *skippedRowsMetricsImpl) Inc(reason pipelines.SkipRowReason) {
	if counter, ok := m.counters[reason]; ok {
		counter.Inc()
	}
}

type noopSkippedRowsMetrics struct{}

func (m *noopSkippedRowsMetrics) Inc(_ pipelines.SkipRowReason) {}

func newSkippedRowsMetrics(logger *slog.Logger, streamName string, registry metrics.Registry) skippedRowsMetrics {
	if registry == nil {
		return &noopSkippedRowsMetrics{}
	}

	result := &skippedRowsMetricsImpl{
		logger:   logger,
		counters: make(map[pipelines.SkipRowReason]metrics.Counter),
	}

	for _, reason := range pipelines.AllSkipRowReasons {
		counter := registry.WithTags(map[string]string{
			"stream": streamName,
			"reason": string(reason),
		}).Counter("tt.stream.skipped_rows")
		counter.Add(0)
		result.counters[reason] = counter
	}

	return result
}
