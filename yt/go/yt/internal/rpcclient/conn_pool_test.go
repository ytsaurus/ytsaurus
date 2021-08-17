package rpcclient

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/bus"
)

func TestLRUConnPool(t *testing.T) {
	origCleanupConn := cleanupConn
	defer func() {
		cleanupConn = origCleanupConn
	}()

	deletions := make(chan struct{})

	deletedConns := &sync.Map{}
	cleanupConn = func(conn *bus.ClientConn) {
		deletedConns.Store(conn, struct{}{})
		deletions <- struct{}{}
	}

	size := 10
	conns := &sync.Map{}
	p := NewLRUConnPool(func(ctx context.Context, addr string) (BusConn, error) {
		conn := &bus.ClientConn{}
		conns.Store(addr, conn)
		return conn, nil
	}, size)

	// Add conns up to the limit.
	for i := 0; i < size; i++ {
		addr := fmt.Sprintf("1.1.1.%v", i)
		_, err := p.Conn(context.Background(), addr)
		require.NoError(t, err)

		_, ok := conns.Load(addr)
		require.True(t, ok)
	}

	checkDeleted := func(t *testing.T, addr string) {
		t.Helper()

		conn, ok := conns.Load(addr)
		require.True(t, ok)

		_, ok = deletedConns.Load(conn)
		require.True(t, ok)
	}

	// Add extra conn and trigger deletion.
	_, err := p.Conn(context.Background(), "2.2.2.2")
	require.NoError(t, err)
	<-deletions
	checkDeleted(t, "1.1.1.0")

	// Discard addr and trigger conn deletion.
	p.Discard("1.1.1.2")
	<-deletions
	checkDeleted(t, "1.1.1.2")
}
