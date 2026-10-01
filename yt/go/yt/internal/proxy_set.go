package internal

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
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
	maxYPClusterNameSize = 32

	// DefaultActiveSetSize is the maximum number of proxies kept in the active set by default.
	DefaultActiveSetSize = 50
)

type ProxyPriority int

const (
	ProxyPriorityLocal ProxyPriority = iota
	ProxyPriorityForeign
	proxyPriorityCount
)

type ProxyPriorityProvider func(address string) ProxyPriority

func NewYPClusterProxyPriorityProvider(localHostName string) (ProxyPriorityProvider, string) {
	localCluster, localClusterOK := inferYPClusterFromHostName(localHostName)

	provider := func(address string) ProxyPriority {
		cluster, ok := inferYPClusterFromHostName(address)
		if localClusterOK && ok && cluster == localCluster {
			return ProxyPriorityLocal
		}
		return ProxyPriorityForeign
	}
	return provider, localCluster
}

func inferYPClusterFromHostName(hostName string) (string, bool) {
	start := strings.IndexByte(hostName, '.')
	if start == -1 {
		return "", false
	}

	end := strings.IndexByte(hostName[start+1:], '.')
	if end == -1 {
		return "", false
	}
	end += start + 1

	cluster := hostName[start+1 : end]
	if cluster == "" || len(cluster) > maxYPClusterNameSize {
		return "", false
	}
	return cluster, true
}

type stringSet struct {
	set   []string
	index map[string]int
}

func (s *stringSet) empty() bool {
	return len(s.set) == 0
}

func (s *stringSet) size() int {
	return len(s.set)
}

func (s *stringSet) random() string {
	return s.set[rand.Intn(len(s.set))]
}

func (s *stringSet) contains(ss string) bool {
	_, ok := s.index[ss]
	return ok
}

func (s *stringSet) add(ss string) bool {
	if s.index == nil {
		s.index = map[string]int{}
	}

	if _, ok := s.index[ss]; ok {
		return false
	}

	s.index[ss] = len(s.set)
	s.set = append(s.set, ss)
	return true
}

func (s *stringSet) remove(ss string) {
	index, ok := s.index[ss]
	if !ok {
		return
	}

	delete(s.index, ss)
	if index+1 == len(s.set) {
		s.set = s.set[:index]
	} else {
		last := len(s.set) - 1

		s.index[s.set[last]] = index
		s.set[index] = s.set[last]
		s.set = s.set[:last]
	}
}

type ProxySet struct {
	UpdatePeriod time.Duration
	BanDuration  time.Duration
	UpdateFn     func() ([]string, error)

	banned sync.Map

	mu sync.Mutex

	updateErr      error
	updateDone     chan struct{}
	updating       bool
	lastUpdateTime time.Time

	ActiveSetSize int

	PriorityProvider                 ProxyPriorityProvider
	MinPeerCountForPriorityAwareness int

	all      stringSet
	active   [proxyPriorityCount]stringSet
	inactive [proxyPriorityCount]stringSet
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
		return DefaultActiveSetSize
	}
}

var errProxyListEmpty = errors.New("proxy list is empty")

func (s *ProxySet) doPickRandom() (string, bool) {
	switch {
	case s.activePeerCount() != 0:
		return s.pickActive(), true
	case !s.all.empty():
		return s.all.random(), true
	default:
		return "", false
	}
}

func (s *ProxySet) pickActive() string {
	index := rand.Intn(min(max(s.MinPeerCountForPriorityAwareness, 1), s.activePeerCount()))
	for priority := ProxyPriorityLocal; priority < proxyPriorityCount; priority++ {
		if index < s.active[priority].size() {
			return s.active[priority].random()
		}
		index -= s.active[priority].size()
	}
	panic("proxy set inconsistent")
}

func (s *ProxySet) priority(proxy string) ProxyPriority {
	if s.PriorityProvider == nil {
		return ProxyPriorityLocal
	}
	return s.PriorityProvider(proxy)
}

func (s *ProxySet) activePeerCount() int {
	return s.active[ProxyPriorityLocal].size() + s.active[ProxyPriorityForeign].size()
}

func (s *ProxySet) updateProxies(updateDone chan struct{}) {
	defer close(updateDone)

	proxyList, err := s.UpdateFn()

	var cleanBan []string
	s.banned.Range(func(key, value any) bool {
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

	var all stringSet
	for _, proxy := range proxyList {
		all.add(proxy)
	}

	alive := []string{}
	for _, proxy := range proxyList {
		if _, banned := s.banned.Load(proxy); banned {
			continue
		}

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
	s.all = all

	for priority := ProxyPriorityLocal; priority < proxyPriorityCount; priority++ {
		for proxy := range s.active[priority].index {
			if !all.contains(proxy) {
				s.active[priority].remove(proxy)
			}
		}

		for proxy := range s.inactive[priority].index {
			if !all.contains(proxy) {
				s.inactive[priority].remove(proxy)
			}
		}
	}

	for _, proxy := range alive {
		priority := s.priority(proxy)
		if !s.active[priority].contains(proxy) {
			s.inactive[priority].add(proxy)
		}
	}

	s.updateActiveSet()
}

func (s *ProxySet) updateActiveSet() {
	remaining := s.activeSetSize()
	for priority := ProxyPriorityLocal; priority < proxyPriorityCount; priority++ {
		active, inactive := &s.active[priority], &s.inactive[priority]
		for active.size() > remaining {
			proxy := active.random()
			active.remove(proxy)
			inactive.add(proxy)
		}
		for active.size() < remaining && !inactive.empty() {
			proxy := inactive.random()
			inactive.remove(proxy)
			active.add(proxy)
		}
		remaining -= active.size()
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

	priority := s.priority(name)
	s.active[priority].remove(name)
	s.inactive[priority].remove(name)
	s.updateActiveSet()
}

type ProxyBouncer struct {
	Log log.Structured

	ProxySet *ProxySet
}

func (b *ProxyBouncer) banProxy(call *Call, err error) {
	if err == nil || call.SelectedProxy == "" {
		return
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) || isProxyBannedError(err) {
		b.Log.Debug("banning proxy", log.String("fqdn", call.SelectedProxy), log.Error(err))
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
