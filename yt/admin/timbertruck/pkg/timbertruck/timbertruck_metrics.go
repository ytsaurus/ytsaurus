package timbertruck

import (
	"log/slog"

	"go.ytsaurus.tech/library/go/core/metrics"
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
		c.logger.Error("Unexpected error ListActiveTasks()", "error", err)
		return
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
