package internal

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"
)

type proxyBan struct {
	at time.Time
}

func (b proxyBan) expired(banDuration time.Duration) bool {
	return time.Since(b.at) > banDuration
}

const (
	defaultUpdatePeriod  = time.Second * 30
	defaultBanDuration   = 5 * time.Minute
	defaultActiveSetSize = 50
)

type ProxySet struct {
	UpdatePeriod time.Duration
	BanDuration  time.Duration
	UpdateFn     func() ([]string, error)

	banned sync.Map

	mu sync.Mutex

	all            []string // all = banned + active + inactive
	updateErr      error
	updateDone     chan struct{}
	updating       bool
	lastUpdateTime time.Time

	ActiveSetSize int
	active        []string
	activeIndex   map[string]int

	inactive      []string
	inactiveIndex map[string]int
}

func (s *ProxySet) updatePeriod() time.Duration {
	if s.UpdatePeriod != 0 {
		return s.UpdatePeriod
	} else {
		return defaultUpdatePeriod
	}
}

func (s *ProxySet) banDuration() time.Duration {
	if s.BanDuration != 0 {
		return s.BanDuration
	} else {
		return defaultBanDuration
	}
}

func (s *ProxySet) activeSetSize() int {
	if s.ActiveSetSize != 0 {
		return s.ActiveSetSize
	} else {
		return defaultActiveSetSize
	}
}

var errProxyListEmpty = errors.New("proxy list is empty")

func (s *ProxySet) doPickRandom() (string, bool) {
	switch {
	case len(s.active) != 0:
		return s.active[rand.Intn(len(s.active))], true

	case len(s.all) != 0:
		return s.all[rand.Intn(len(s.all))], true

	default:
		return "", false
	}
}

func (s *ProxySet) updateProxies(updateDone chan struct{}) {
	defer close(updateDone)

	proxyList, err := s.UpdateFn()

	var cleanBan []string
	s.banned.Range(func(key, value interface{}) bool {
		proxy := key.(string)
		ban := value.(proxyBan)

		if ban.expired(s.banDuration()) {
			cleanBan = append(cleanBan, proxy)
		}

		return true
	})

	for _, proxy := range cleanBan {
		s.banned.Delete(proxy)
	}

	alive := []string{}
	aliveIndex := map[string]int{}

	for _, proxy := range proxyList {
		if _, banned := s.banned.Load(proxy); banned {
			continue
		}

		aliveIndex[proxy] = len(alive)
		alive = append(alive, proxy)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.updating = false

	if err != nil {
		s.updateErr = err
		return
	}

	if len(proxyList) == 0 {
		s.updateErr = errProxyListEmpty
		return
	}

	s.updateErr = nil
	s.all = proxyList

	s.updateInactiveSet(alive)
	s.updateActiveSet()
}

func (s *ProxySet) updateInactiveSet(alive []string) {
	inactive := []string{}
	inactiveIndex := map[string]int{}
	for _, proxy := range alive {
		if _, ok := s.activeIndex[proxy]; !ok {
			inactiveIndex[proxy] = len(inactive)
			inactive = append(inactive, proxy)
		}
	}

	s.inactive = inactive
	s.inactiveIndex = inactiveIndex
}

func (s *ProxySet) updateActiveSet() {
	for len(s.active) < s.activeSetSize() && len(s.inactive) > 0 {
		proxy := s.inactive[rand.Intn(len(s.inactive))]

		s.removeInactive(proxy)

		if s.activeIndex == nil {
			s.activeIndex = map[string]int{}
		}

		s.activeIndex[proxy] = len(s.active)
		s.active = append(s.active, proxy)
	}
}

func (s *ProxySet) scheduleUpdate(force bool) (updateDone <-chan struct{}) {
	if s.updating {
		return s.updateDone
	}

	if !force && time.Since(s.lastUpdateTime) < s.updatePeriod() {
		return s.updateDone
	}

	s.lastUpdateTime = time.Now()
	s.updating = true
	s.updateDone = make(chan struct{})
	go s.updateProxies(s.updateDone)
	return s.updateDone
}

func (s *ProxySet) PickRandom(ctx context.Context) (string, error) {
	s.mu.Lock()
	proxy, ok := s.doPickRandom()
	waitUpdate := s.scheduleUpdate(!ok)
	s.mu.Unlock()

	if ok {
		return proxy, nil
	}

	select {
	case <-waitUpdate:
	case <-ctx.Done():
		return "", ctx.Err()
	}

	s.mu.Lock()
	proxy, ok = s.doPickRandom()
	updateErr := s.updateErr
	s.mu.Unlock()

	if !ok && updateErr == nil {
		panic("proxy set inconsistent")
	}

	if ok {
		return proxy, nil
	} else {
		return "", updateErr
	}
}

func (s *ProxySet) BanProxy(name string) {
	ban := proxyBan{at: time.Now()}
	s.banned.Store(name, ban)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.removeActive(name)
	s.removeInactive(name)

	s.updateActiveSet()
}

func (s *ProxySet) removeActive(proxy string) {
	bannedIndex, ok := s.activeIndex[proxy]
	if !ok {
		return
	}

	delete(s.activeIndex, proxy)
	if bannedIndex+1 == len(s.active) {
		s.active = s.active[:bannedIndex]
	} else {
		last := len(s.active) - 1

		s.activeIndex[s.active[last]] = bannedIndex
		s.active[bannedIndex] = s.active[last]
		s.active = s.active[:last]
	}
}

func (s *ProxySet) removeInactive(proxy string) {
	bannedIndex, ok := s.inactiveIndex[proxy]
	if !ok {
		return
	}

	delete(s.inactiveIndex, proxy)
	if bannedIndex+1 == len(s.inactive) {
		s.inactive = s.inactive[:bannedIndex]
	} else {
		last := len(s.inactive) - 1

		s.inactiveIndex[s.inactive[last]] = bannedIndex
		s.inactive[bannedIndex] = s.inactive[last]
		s.inactive = s.inactive[:last]
	}
}

type ProxyBouncer struct {
	ProxySet *ProxySet
}

func (b *ProxyBouncer) banProxy(call *Call, err error) {
	if err == nil || call.SelectedProxy == "" {
		return
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) || isProxyBannedError(err) {
		b.ProxySet.BanProxy(call.SelectedProxy)
	}
}

func (b *ProxyBouncer) Intercept(ctx context.Context, call *Call, next CallInvoker) (res *CallResult, err error) {
	res, err = next(ctx, call)
	b.banProxy(call, err)
	return
}

func (b *ProxyBouncer) Read(ctx context.Context, call *Call, next ReadInvoker) (r io.ReadCloser, err error) {
	r, err = next(ctx, call)
	b.banProxy(call, err)
	if r != nil {
		r = &readerWrapper{b: b, call: call, r: r}
	}
	return
}

func (b *ProxyBouncer) Write(ctx context.Context, call *Call, next WriteInvoker) (w io.WriteCloser, err error) {
	w, err = next(ctx, call)
	b.banProxy(call, err)
	if w != nil {
		w = &writerWrapper{b: b, call: call, w: w}
	}
	return
}

type readerWrapper struct {
	b    *ProxyBouncer
	call *Call
	r    io.ReadCloser
}

func (w *readerWrapper) Read(p []byte) (n int, err error) {
	n, err = w.r.Read(p)
	if err != nil && err != io.EOF {
		w.b.banProxy(w.call, err)
	}
	return
}

func (w *readerWrapper) Close() error {
	// No point in inspecting this error.
	// This method is used to signal that client is done reading the body.
	return w.r.Close()
}

type writerWrapper struct {
	b    *ProxyBouncer
	call *Call
	w    io.WriteCloser
}

func (w *writerWrapper) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	if err != nil {
		w.b.banProxy(w.call, err)
	}
	return
}

func (w *writerWrapper) Close() error {
	err := w.w.Close()
	if err != nil {
		w.b.banProxy(w.call, err)
	}
	return err
}
