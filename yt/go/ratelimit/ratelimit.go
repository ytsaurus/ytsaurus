package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Limiter is precise rate limiter with context support.
type Limiter struct {
	maxSize  int
	interval time.Duration

	// invariant: size == maxSize || waiting.empty()
	mu              sync.Mutex
	size            int
	active, waiting waiter
	blocked         bool
}

func (w *waiter) unlink() {
	w.prev.next = w.next
	w.next.prev = w.prev
}

func (w *waiter) link(next *waiter) {
	next.prev = w
	next.next = w.next

	w.next.prev = next
	w.next = next
}

func (w *waiter) empty() bool {
	return w.next == w
}

func (w *waiter) init() {
	w.next = w
	w.prev = w
}

type waiter struct {
	prev, next *waiter
	wakeup     chan struct{}
	at         time.Time
}

// NewLimiter returns limiter that throttles rate of successful Acquire() calls
// to maxSize events at any given interval.
func NewLimiter(maxCount int, interval time.Duration) *Limiter {
	l := &Limiter{
		maxSize:  maxCount,
		interval: interval,
	}

	l.active.init()
	l.waiting.init()
	return l
}

const enableCheck = true

func (l *Limiter) check() {
	if enableCheck {
		if l.size != l.maxSize && !l.waiting.empty() {
			panic("inconsistent limiter state")
		}
	}
}

func (l *Limiter) scheduleWakeup() {
	if l.blocked {
		return
	}

	l.blocked = true
	go l.wakeupBlocked()
}

func (l *Limiter) wakeupBlocked() {
	l.mu.Lock()
	l.check()

	for {
		if l.waiting.empty() {
			l.blocked = false
			l.mu.Unlock()
			return
		}

		earliest := l.active.next.at
		nextWakeup := earliest.Add(l.interval)

		l.mu.Unlock()
		time.Sleep(time.Until(nextWakeup))
		l.mu.Lock()
		l.check()

		l.cleanup(nextWakeup.Add(-l.interval))
		for l.size < l.maxSize && !l.waiting.empty() {
			first := l.waiting.next
			close(first.wakeup)
			first.unlink()
			first.at = nextWakeup
			l.size++
			l.active.prev.link(first)
		}
	}
}

func (l *Limiter) cleanup(before time.Time) {
	for !l.active.empty() {
		head := l.active.next

		if head.at.After(before) {
			break
		}

		head.unlink()
		l.size--
	}
}

func (l *Limiter) Acquire(ctx context.Context) error {
	now := time.Now()
	past := now.Add(-l.interval)

	me := &waiter{
		wakeup: make(chan struct{}),
	}

	acquired := false

	startWait := func() {
		l.waiting.prev.link(me)
		l.scheduleWakeup()
	}

	{
		l.mu.Lock()
		l.check()

		if !l.waiting.empty() {
			startWait()
		} else {
			l.cleanup(past)

			if l.size < l.maxSize {
				l.size++
				me.at = now
				acquired = true
				l.active.prev.link(me)
			} else {
				startWait()
			}
		}
		l.mu.Unlock()
	}

	if acquired {
		return nil
	}

	select {
	case <-me.wakeup:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		l.check()

		if me.at.IsZero() {
			me.unlink()
			l.mu.Unlock()

			return ctx.Err()
		}

		l.mu.Unlock()
		return nil
	}
}
