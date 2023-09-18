package rpcclient

import (
	"context"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
)

const idleConnTimeout = time.Minute

var cleanupConn = closeConn

func closeConn(conn *Conn) {
	conn.Close()
}

type ConnPool struct {
	dialFunc Dialer

	mu    sync.Mutex
	conns map[string]*Conn

	stop chan struct{}

	IdleConnTimeout time.Duration

	log log.Structured
}

// NewConnPool creates connection pool and starts periodic gc.
func NewConnPool(dialFunc Dialer, log log.Structured) *ConnPool {
	pool := &ConnPool{
		dialFunc:        dialFunc,
		conns:           make(map[string]*Conn),
		stop:            make(chan struct{}),
		IdleConnTimeout: idleConnTimeout,
		log:             log,
	}

	go pool.run()

	return pool
}

func (p *ConnPool) run() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			p.gc()
		case <-p.stop:
			return
		}
	}
}

func (p *ConnPool) gc() {
	p.log.Debug("gc started")

	p.mu.Lock()
	var deleted []*Conn
	for addr, conn := range p.conns {
		if conn.Expired() {
			p.log.Debug("gc: conn expired -> deleting",
				log.String("addr", addr),
				log.Time("expires_at", conn.expiresAt),
				log.Int("inflight", conn.inflight))

			delete(p.conns, addr)
			deleted = append(deleted, conn)
		} else {
			p.log.Debug("gc: conn not expired",
				log.String("addr", addr),
				log.Int("inflight", conn.inflight))
		}
	}
	p.mu.Unlock()

	for _, conn := range deleted {
		cleanupConn(conn)
	}

	p.log.Debug("gc finished")
}

func (p *ConnPool) idleConnTimeout() time.Duration {
	if p.IdleConnTimeout != 0 {
		return p.IdleConnTimeout
	} else {
		return idleConnTimeout
	}
}

// Conn returns a connection from the pool.
//
// Uses cached conn if present and dials otherwise.
func (p *ConnPool) Conn(ctx context.Context, addr string) (*Conn, error) {
	p.mu.Lock()

	p.log.Debug("getting conn", log.String("addr", addr))

	var expiredConn *Conn
	conn, ok := p.conns[addr]
	if !ok || conn.Expired() || conn.Err() != nil {
		expiredConn, conn = conn, nil
	}

	if conn != nil {
		p.log.Debug("got conn from cache", log.String("addr", addr))
		conn.inflight++

		p.mu.Unlock()
		return conn, nil
	}

	p.log.Debug("unable to got conn from cache; dialing", log.String("addr", addr))
	c := p.dialFunc(ctx, addr)

	p.log.Debug("dialed", log.String("addr", addr))
	wrapped := NewConn(addr, c, p)
	p.conns[addr] = wrapped
	wrapped.inflight++

	p.mu.Unlock()

	if expiredConn != nil {
		cleanupConn(expiredConn)
	}

	return wrapped, nil
}

// Discard removes connection from the connection pool and closes it.
func (p *ConnPool) Discard(addr string) {
	p.log.Debug("discarding conn", log.String("addr", addr))

	p.mu.Lock()
	conn, ok := p.conns[addr]
	delete(p.conns, addr)
	p.mu.Unlock()

	if ok {
		cleanupConn(conn)
	}
}

func (p *ConnPool) discard(conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.log.Debug("discarding conn", log.String("addr", conn.addr))

	if poolConn, ok := p.conns[conn.addr]; ok && conn == poolConn {
		delete(p.conns, conn.addr)
	}

	cleanupConn(conn)
}

func (p *ConnPool) tryPutIdleConn(conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn.inflight--
	conn.expiresAt = time.Now().Add(p.idleConnTimeout())

	p.log.Debug("put conn back to pool",
		log.String("addr", conn.addr),
		log.Time("expires_at", conn.expiresAt),
		log.Int("inflight", conn.inflight),
	)
}

// Stop closes every connection in the pool and stops gc.
func (p *ConnPool) Stop() {
	close(p.stop)

	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conn := range p.conns {
		delete(p.conns, addr)
		cleanupConn(conn)
	}
}

type Conn struct {
	BusConn

	addr string
	pool *ConnPool

	inflight  int
	expiresAt time.Time
}

func NewConn(addr string, c BusConn, pool *ConnPool) *Conn {
	return &Conn{
		addr:    addr,
		BusConn: c,
		pool:    pool,
	}
}

func (c *Conn) Release() {
	if c.pool == nil {
		return
	}

	c.pool.tryPutIdleConn(c)
}

func (c *Conn) Discard() {
	c.pool.discard(c)
}

func (c *Conn) Expired() bool {
	return c.inflight == 0 && time.Now().After(c.expiresAt)
}
