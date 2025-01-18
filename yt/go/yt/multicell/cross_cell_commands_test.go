package multicell

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/clienttest"
	"go.ytsaurus.tech/yt/go/yt/internal/httpclient"
	"go.ytsaurus.tech/yt/go/yt/internal/rpcclient"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestCrossCellCommandsRetries(t *testing.T) {
	env := yttest.New(t)

	portalEntrance := ypath.Path("//tmp/some-portal")
	portalCellID := 2
	yc := clienttest.NewHTTPClient(t, env.L)
	_, err := yc.CreateNode(env.Ctx, portalEntrance, yt.NodePortalEntrance,
		&yt.CreateNodeOptions{Attributes: map[string]any{"exit_cell_tag": portalCellID}})
	require.NoError(t, err)

	for _, tc := range []struct {
		name           string
		yc             yt.Client
		runCommand     func(ctx context.Context, yc yt.Client, src, dst ypath.YPath) (yt.NodeID, error)
		getPreparedCtx func(t *testing.T, ytEnv *yttest.Env, retriesCount *atomic.Uint32) (ctx context.Context, connect chan struct{})
	}{
		{
			name: "cross-cell-copy-http",
			yc:   clienttest.NewHTTPClient(t, env.L),
			runCommand: func(ctx context.Context, yc yt.Client, src, dst ypath.YPath) (yt.NodeID, error) {
				return yc.CopyNode(ctx, src, dst, nil)
			},
			getPreparedCtx: prepareCtxWithDisconnectedRoundTripper,
		},
		{
			name: "cross-cell-copy-rpc",
			yc:   clienttest.NewRPCClient(t, env.L),
			runCommand: func(ctx context.Context, yc yt.Client, src, dst ypath.YPath) (yt.NodeID, error) {
				return yc.CopyNode(ctx, src, dst, nil)
			},
			getPreparedCtx: prepareCtxWithDisconnectedRPCConnDialer,
		},
		{
			name: "cross-cell-move-http",
			yc:   clienttest.NewHTTPClient(t, env.L),
			runCommand: func(ctx context.Context, yc yt.Client, src, dst ypath.YPath) (yt.NodeID, error) {
				return yc.MoveNode(ctx, src, dst, nil)
			},
			getPreparedCtx: prepareCtxWithDisconnectedRoundTripper,
		},
		{
			name: "cross-cell-move-rpc",
			yc:   clienttest.NewRPCClient(t, env.L),
			runCommand: func(ctx context.Context, yc yt.Client, src, dst ypath.YPath) (yt.NodeID, error) {
				return yc.MoveNode(ctx, src, dst, nil)
			},
			getPreparedCtx: prepareCtxWithDisconnectedRPCConnDialer,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srcPath := portalEntrance.Child(fmt.Sprintf("src-table-%s", tc.name))
			_, err = yc.CreateNode(env.Ctx, srcPath, yt.NodeTable, nil)
			require.NoError(t, err)

			dstPath := env.TmpPath()

			var retriesCount atomic.Uint32
			ctx, connect := tc.getPreparedCtx(t, env, &retriesCount)
			commandErrors := make(chan error, 1)
			go func() {
				_, err := tc.runCommand(ctx, tc.yc, srcPath, dstPath)
				commandErrors <- err
			}()

			select {
			case err := <-commandErrors:
				t.Fatalf("command completed before connection is established: %s", err)
			case <-time.After(time.Second * 5):
			}

			rtCount := retriesCount.Load()
			require.Greater(t, rtCount, uint32(1))

			close(connect)

			select {
			case err := <-commandErrors:
				require.NoError(t, err)

				exists, err := tc.yc.NodeExists(env.Ctx, dstPath, nil)
				require.NoError(t, err)
				require.True(t, exists)
			case <-time.After(time.Second * 20):
				t.Fatal("command does not completed after connection is established")
			}
		})
	}
}

type timeoutNetError struct {
}

func (e *timeoutNetError) Error() string {
	return "timeout net error"
}

func (e *timeoutNetError) Timeout() bool {
	return true
}

func (e *timeoutNetError) Temporary() bool {
	return true
}

var _ net.Error = &timeoutNetError{}

func prepareCtxWithDisconnectedRoundTripper(t *testing.T, ytEnv *yttest.Env, retriesCount *atomic.Uint32) (context.Context, chan struct{}) {
	t.Helper()

	drt := &disconnectedRoundTripper{
		connect: make(chan struct{}),
		rtCount: retriesCount,
	}
	ctx := httpclient.WithRoundTripper(ytEnv.Ctx, drt)

	return ctx, drt.connect
}

func prepareCtxWithDisconnectedRPCConnDialer(t *testing.T, ytEnv *yttest.Env, retriesCount *atomic.Uint32) (context.Context, chan struct{}) {
	t.Helper()

	connect := make(chan struct{})
	dialer := newDisconnectedRPCConnDialer(t, connect, retriesCount)
	ctx := rpcclient.WithDialer(ytEnv.Ctx, dialer)

	return ctx, connect
}

type disconnectedRoundTripper struct {
	connect chan struct{}
	rtCount *atomic.Uint32
}

func (d *disconnectedRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	d.rtCount.Add(1)
	select {
	case <-d.connect:
		return http.DefaultClient.Do(req)
	default:
		return nil, &timeoutNetError{}
	}
}

var _ http.RoundTripper = &disconnectedRoundTripper{}

type disconnectedRPCConn struct {
	conn    rpcclient.BusConn
	connect chan struct{}
}

func newDisconnectedRPCConn(ctx context.Context, addr string, connect chan struct{}) *disconnectedRPCConn {
	conn := rpcclient.DefaultDial(ctx, addr)

	ret := &disconnectedRPCConn{
		conn:    conn,
		connect: connect,
	}

	return ret
}

func (c *disconnectedRPCConn) Send(
	ctx context.Context,
	service, method string,
	request, reply proto.Message,
	opts ...bus.SendOption,
) error {
	select {
	case <-c.connect:
		return c.conn.Send(ctx, service, method, request, reply, opts...)
	default:
		return &timeoutNetError{}
	}
}

func (c *disconnectedRPCConn) Err() error {
	return c.conn.Err()
}

func (c *disconnectedRPCConn) Close() {
	c.conn.Close()
}

func (c *disconnectedRPCConn) Done() <-chan struct{} {
	return c.conn.Done()
}

func newDisconnectedRPCConnDialer(t *testing.T, connect chan struct{}, retriesCount *atomic.Uint32) rpcclient.Dialer {
	t.Helper()

	return func(ctx context.Context, addr string) rpcclient.BusConn {
		retriesCount.Add(1)
		return newDisconnectedRPCConn(ctx, addr, connect)
	}
}
