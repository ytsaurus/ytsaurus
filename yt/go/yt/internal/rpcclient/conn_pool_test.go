package rpcclient

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/bus"
	"a.yandex-team.ru/yt/go/ytlog"
)

func TestConnPool(t *testing.T) {
	origCleanupConn := cleanupConn
	defer func() {
		cleanupConn = origCleanupConn
	}()

	deletedConns := &sync.Map{}
	cleanupConn = func(conn *conn) {
		deletedConns.Store(conn.BusConn, struct{}{})
	}

	conns := &sync.Map{}
	p := NewConnPool(func(ctx context.Context, addr string) BusConn {
		conn := &bus.ClientConn{}
		conns.Store(addr, conn)
		return conn
	}, ytlog.Must())

	const size = 10

	// Add some conns.
	for i := 0; i < size; i++ {
		addr := fmt.Sprintf("1.1.1.%v", i)
		conn, err := p.Conn(context.Background(), addr)
		require.NoError(t, err)
		require.True(t, conn.inflight > 0)
		require.False(t, conn.Expired())

		_, ok := conns.Load(addr)
		require.True(t, ok)
	}

	// Release conn.
	conn, err := p.Conn(context.Background(), "1.1.1.0")
	require.NoError(t, err)
	require.Equal(t, 2, conn.inflight)
	et := conn.expiresAt

	conn.Release()
	require.Equal(t, 1, conn.inflight, "Release should decrement inflight request count.")
	require.False(t, conn.Expired())
	require.True(t, conn.expiresAt.After(et), "Release should extend expiration time.")

	checkDeleted := func(t *testing.T, addr string) {
		t.Helper()

		conn, ok := conns.Load(addr)
		require.True(t, ok)

		_, ok = deletedConns.Load(conn)
		require.True(t, ok, addr)
	}

	// Discard conn.
	conn.Discard()
	checkDeleted(t, "1.1.1.0")

	// Discard addr and trigger conn deletion.
	p.Discard("1.1.1.1")
	checkDeleted(t, "1.1.1.1")
	// Second discard does nothing.
	p.Discard("1.1.1.1")

	// Shutdown pool.
	p.Stop()
	for i := 1; i < size; i++ {
		addr := fmt.Sprintf("1.1.1.%v", i)
		checkDeleted(t, addr)
	}
}
