package main

import "sync"

type ActorMetrics struct {
	SuccessChecks int64
	FailChecks    int64
	CacheHit      int64
}

type PerActorMetrics struct {
	metrics map[string]*ActorMetrics
	mutex   sync.Mutex
}

var Metrics PerActorMetrics = PerActorMetrics{
	metrics: map[string]*ActorMetrics{},
}

func (p *PerActorMetrics) Reset() (result map[string]*ActorMetrics) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	result = p.metrics
	p.metrics = make(map[string]*ActorMetrics)
	return
}

func (p *PerActorMetrics) Update(actor string, metricsIncrement ActorMetrics) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if metrics, ok := p.metrics[actor]; ok {
		metrics.SuccessChecks += metricsIncrement.SuccessChecks
		metrics.FailChecks += metricsIncrement.FailChecks
		metrics.CacheHit += metricsIncrement.CacheHit
	} else {
		p.metrics[actor] = &metricsIncrement
	}
}
