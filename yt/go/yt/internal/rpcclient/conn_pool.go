package rpcclient

import (
	"context"
	"fmt"
	"time"

	"github.com/karlseguin/ccache"

	"a.yandex-team.ru/yt/go/bus"
)

const (
	connTTL      = time.Hour
	connPoolSize = 50
)

type ConnPool interface {
	// Conn returns a ClientConn from the pool.
	Conn(ctx context.Context, addr string) (*bus.ClientConn, error)

	// Discard removes connection from the connection pool and closes it.
	Discard(addr string)

	// Close closes every ClientConn in the pool.
	Close() error
}

type dialFunc func(ctx context.Context, addr string) (*bus.ClientConn, error)

type lruConnPool struct {
	dialFunc dialFunc
	cache    *ccache.Cache
}

func NewLRUConnPool(dialFunc dialFunc, size int) *lruConnPool {
	p := &lruConnPool{
		dialFunc: dialFunc,
		cache: ccache.New(ccache.Configure().
			MaxSize(int64(size)).
			ItemsToPrune(1).
			OnDelete(func(item *ccache.Item) {
				conn := item.Value().(*bus.ClientConn)
				cleanupConn(conn)
			}),
		),
	}

	return p
}

var cleanupConn = closeConn

func closeConn(conn *bus.ClientConn) {
	conn.Close()
	<-conn.Done()
}

func (p *lruConnPool) Conn(ctx context.Context, addr string) (*bus.ClientConn, error) {
	item, err := p.cache.Fetch(addr, connTTL, func() (i interface{}, e error) {
		return p.dialFunc(ctx, addr)
	})

	if err != nil {
		return nil, err
	}

	return item.Value().(*bus.ClientConn), nil
}

func (p *lruConnPool) Discard(addr string) {
	deleted := p.cache.Delete(addr)
	fmt.Println("deleted:", deleted)
}

func (p *lruConnPool) Close() error {
	p.cache.DeletePrefix("")
	p.cache.Stop()
	return nil
}
