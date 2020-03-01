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
	if l.done {
		l.m.Unlock()
		return false
	}

	l.count++
	l.m.Unlock()
	return true
}

func (l *StopGroup) Done() {
	l.m.Lock()
	l.count--

	if l.count == 0 {
		l.finished.Broadcast()
	}
	l.m.Unlock()
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
	if !l.done {
		l.done = true
		close(l.c)
	}

	for l.count > 0 {
		l.finished.Wait()
	}

	l.m.Unlock()
}

func NewStopGroup() *StopGroup {
	s := &StopGroup{c: make(chan struct{})}
	s.finished = sync.NewCond(&s.m)
	return s
}
