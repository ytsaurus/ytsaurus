package internal

import (
	"math/rand"
	"sync"
	"time"
)

type BackoffStrategy interface {
	Backoff(i int) time.Duration
}

type ExpBackoff struct {
	InitialBackoff, MaxBackoff time.Duration
	Multiplier, Jitter         float64

	l   sync.Mutex
	rng *rand.Rand
}

func (e *ExpBackoff) Backoff(i int) time.Duration {
	backoff := e.InitialBackoff

	for j := 0; j < i; j++ {
		backoff = time.Nanosecond * time.Duration(float64(backoff.Nanoseconds())*e.Multiplier)

		if backoff > e.MaxBackoff {
			backoff = e.MaxBackoff
			break
		}
	}

	e.l.Lock()
	jitter := e.rng.Float64() - 0.5
	e.l.Unlock()

	scale := 1 + 2*(jitter*e.Jitter)

	return time.Nanosecond * time.Duration(float64(backoff.Nanoseconds())*scale)
}

var DefaultBackoff = ExpBackoff{
	InitialBackoff: time.Second,
	MaxBackoff:     2 * time.Minute,
	Multiplier:     1.6,
	Jitter:         0.2,

	rng: rand.New(rand.NewSource(time.Now().UnixNano())),
}
