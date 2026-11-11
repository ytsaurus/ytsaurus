package timbertruck

import (
	"log/slog"
	"sync"
	"sync/atomic"

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

// rowCounters holds a counter per value of a single tag. It is a noop when registry is nil.
type rowCounters[T ~string] struct {
	counters map[T]metrics.Counter
}

func (c rowCounters[T]) Inc(value T) {
	if counter, ok := c.counters[value]; ok {
		counter.Inc()
	}
}

func newRowCounters[T ~string](registry metrics.Registry, streamName string, name string, tag string, values []T) rowCounters[T] {
	result := rowCounters[T]{counters: make(map[T]metrics.Counter, len(values))}
	if registry == nil {
		return result
	}

	for _, value := range values {
		counter := registry.WithTags(map[string]string{
			"stream": streamName,
			tag:      string(value),
		}).Counter(name)
		counter.Add(0)
		result.counters[value] = counter
	}

	return result
}

// rowMaxGauges holds a gauge per value of a single tag. It is a noop when registry is nil.
type rowMaxGauges[T ~string] struct {
	maxes map[T]*periodMax
}

func (g rowMaxGauges[T]) Observe(value T, size int) {
	if m, ok := g.maxes[value]; ok {
		m.observe(int64(size))
	}
}

func (g rowMaxGauges[T]) Rotate() {
	for _, m := range g.maxes {
		m.rotate()
	}
}

func newRowMaxGauges[T ~string](registry metrics.Registry, streamName string, name string, tag string, values []T) rowMaxGauges[T] {
	result := rowMaxGauges[T]{maxes: make(map[T]*periodMax, len(values))}
	if registry == nil {
		return result
	}

	for _, value := range values {
		m := &periodMax{}
		registry.WithTags(map[string]string{
			"stream": streamName,
			tag:      string(value),
		}).FuncIntGauge(name, m.value)
		result.maxes[value] = m
	}

	return result
}

func newStreamMaxGauge(registry metrics.Registry, streamName string, name string) *periodMax {
	result := &periodMax{}
	if registry != nil {
		registry.WithTags(map[string]string{"stream": streamName}).FuncIntGauge(name, result.value)
	}
	return result
}

type periodMax struct {
	mu       sync.Mutex
	current  atomic.Int64
	previous int64
}

func (m *periodMax) observe(value int64) {
	for current := m.current.Load(); value > current; current = m.current.Load() {
		if m.current.CompareAndSwap(current, value) {
			return
		}
	}
}

func (m *periodMax) rotate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.previous = m.current.Swap(0)
}

func (m *periodMax) value() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return max(m.previous, m.current.Load())
}
