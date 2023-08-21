package internal

import (
	"context"
	"sync"
)

type StopGroup struct {
	m        sync.Mutex
	finished *sync.Cond

	count int
	done  bool

	c chan struct{}
}

func (l *StopGroup) TryAdd() bool {
	l.m.Lock()
	defer l.m.Unlock()

	if l.done {
		return false
	}

	l.count++
	return true
}

func (l *StopGroup) Done() {
	l.m.Lock()
	defer l.m.Unlock()

	l.count--

	if l.count == 0 {
		l.finished.Broadcast()
	}
}

func (l *StopGroup) C() <-chan struct{} {
	return l.c
}

type stopContext struct {
	context.Context
	l *StopGroup
}

func (ctx *stopContext) Done() <-chan struct{} {
	return ctx.l.c
}

func (ctx *stopContext) Err() error {
	select {
	case <-ctx.l.c:
		return context.Canceled
	default:
		return nil
	}
}

func (l *StopGroup) Context() context.Context {
	return &stopContext{
		Context: context.Background(),
		l:       l,
	}
}

func (l *StopGroup) Stop() {
	l.m.Lock()
	defer l.m.Unlock()

	if !l.done {
		l.done = true
		close(l.c)
	}

	for l.count > 0 {
		l.finished.Wait()
	}
}

func NewStopGroup() *StopGroup {
	s := &StopGroup{c: make(chan struct{})}
	s.finished = sync.NewCond(&s.m)
	return s
}
