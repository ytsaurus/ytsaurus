package integration

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal/httpclient"
	"go.ytsaurus.tech/yt/go/yt/internal/rpcclient"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestTransactions(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "CommitTransaction", Test: suite.TestCommitTransaction},
		{Name: "RollbackTransaction", Test: suite.TestRollbackTransaction},
		{Name: "TransactionBackgroundPing", Test: suite.TestTransactionBackgroundPing},
		{Name: "TransactionAbortByContextCancel", Test: suite.TestTransactionAbortByContextCancel},
		{Name: "TransactionAbortCancel", Test: suite.TestTransactionAbortCancel},
		{Name: "NestedTransactions", Test: suite.TestNestedTransactions},
		{Name: "TestExecTx_retries", Test: suite.TestExecTx_retries},
	})
}

func (s *Suite) TestCommitTransaction(t *testing.T, yc yt.Client) {
	tx, err := yc.BeginTx(s.Ctx, nil)
	require.NoError(t, err)

	name := tmpPath()
	_, err = tx.CreateNode(s.Ctx, name, yt.NodeMap, nil)
	require.NoError(t, err)

	ok, err := yc.NodeExists(s.Ctx, name, nil)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, tx.Commit())

	ok, err = yc.NodeExists(s.Ctx, name, nil)
	require.NoError(t, err)
	require.True(t, ok)
}

func (s *Suite) TestRollbackTransaction(t *testing.T, yc yt.Client) {
	tx, err := yc.BeginTx(s.Ctx, nil)
	require.NoError(t, err)

	require.NoError(t, tx.Abort())
}

func (s *Suite) TestTransactionBackgroundPing(t *testing.T, yc yt.Client) {
	lowTimeout := yson.Duration(5 * time.Second)
	tx, err := yc.BeginTx(s.Ctx, &yt.StartTxOptions{Timeout: &lowTimeout})
	require.NoError(t, err)

	time.Sleep(time.Second * 10)

	require.NoError(t, tx.Commit())
}

func (s *Suite) TestTransactionAbortByContextCancel(t *testing.T, yc yt.Client) {
	canceledCtx, cancel := context.WithCancel(s.Ctx)

	tx, err := yc.BeginTx(canceledCtx, nil)
	require.NoError(t, err)

	cancel()
	time.Sleep(time.Second)

	require.Error(t, tx.Commit())

	err = yc.PingTx(s.Ctx, tx.ID(), nil)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
}

func (s *Suite) TestTransactionAbortCancel(t *testing.T, yc yt.Client) {
	ctx, cancel := context.WithCancel(s.Ctx)
	defer cancel()

	tx, err := yc.BeginTx(ctx, nil)
	require.NoError(t, err)

	go func() {
		<-tx.Finished()
		cancel()
	}()

	require.NoError(t, tx.Abort())
}

func (s *Suite) TestNestedTransactions(t *testing.T, yc yt.Client) {
	rootTx, err := yc.BeginTx(s.Ctx, nil)
	require.NoError(t, err)

	nestedTx, err := rootTx.BeginTx(s.Ctx, nil)
	require.NoError(t, err)

	p := tmpPath()

	_, err = nestedTx.CreateNode(s.Ctx, p, yt.NodeMap, nil)
	require.NoError(t, err)

	ok, err := rootTx.NodeExists(s.Ctx, p, nil)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, nestedTx.Commit())

	ok, err = rootTx.NodeExists(s.Ctx, p, nil)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = yc.NodeExists(s.Ctx, p, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func (s *Suite) TestExecTx_retries(t *testing.T, yc yt.Client) {
	t.Parallel()

	wrapExecTx := func(ctx context.Context, cb func() error, opts yt.ExecTxRetryOptions) error {
		return yt.ExecTx(ctx, yc, func(ctx context.Context, tx yt.Tx) error {
			return cb()
		}, &yt.ExecTxOptions{RetryOptions: opts})
	}

	wrapExecTabletTx := func(ctx context.Context, cb func() error, opts yt.ExecTxRetryOptions) error {
		return yt.ExecTabletTx(s.Ctx, yc, func(ctx context.Context, tx yt.TabletTx) error {
			return cb()
		}, &yt.ExecTabletTxOptions{RetryOptions: opts})
	}

	wrappers := map[string]func(ctx context.Context, cb func() error, opts yt.ExecTxRetryOptions) error{
		"master": wrapExecTx,
		"tablet": wrapExecTabletTx,
	}

	for txType, execTx := range wrappers {
		t.Run(txType, func(t *testing.T) {
			t.Run("simple", func(t *testing.T) {
				v := 0
				err := execTx(s.Ctx, func() error {
					v++
					if v <= 2 {
						return xerrors.New("some error")
					}
					return nil
				}, nil)
				require.NoError(t, err)
				require.Equal(t, 1+2, v)
			})

			t.Run("default retry options", func(t *testing.T) {
				v := 0
				err := execTx(s.Ctx, func() error {
					v++
					return xerrors.New("some error")
				}, nil)
				require.Error(t, err)
				require.Equal(t, 1+yt.DefaultExecTxRetryCount, v)
			})

			t.Run("no retries", func(t *testing.T) {
				v := 0
				err := execTx(s.Ctx, func() error {
					v++
					return xerrors.New("some error")
				}, &yt.ExecTxRetryOptionsNone{})
				require.Error(t, err)
				require.Equal(t, 1, v)
			})

			t.Run("retry cancellation", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()

				v := 0
				err := execTx(ctx, func() error {
					v++
					return xerrors.New("some error")
				}, nil)
				require.Error(t, err)
				require.True(t, v >= 3)
			})
		})
	}
}

type disconnectingRoundTripper struct {
	disconnect chan struct{}
}

func (d *disconnectingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	select {
	case <-d.disconnect:
		return nil, fmt.Errorf("network unavailable")
	default:
		return http.DefaultClient.Do(req)
	}
}

type disconnectingRPCConn struct {
	conn       rpcclient.BusConn
	disconnect chan struct{}
}

func NewDisconnectingRPCConn(ctx context.Context, addr string, disconnect chan struct{}) *disconnectingRPCConn {
	conn := rpcclient.DefaultDial(ctx, addr)

	ret := &disconnectingRPCConn{
		conn:       conn,
		disconnect: disconnect,
	}

	return ret
}

func (c *disconnectingRPCConn) Send(
	ctx context.Context,
	service, method string,
	request, reply proto.Message,
	opts ...bus.SendOption,
) error {
	select {
	case <-c.disconnect:
		return fmt.Errorf("network unavailable")
	default:
		return c.conn.Send(ctx, service, method, request, reply, opts...)
	}
}

func (c *disconnectingRPCConn) Err() error {
	return c.conn.Err()
}

func (c *disconnectingRPCConn) Close() {
	c.conn.Close()
}

func (c *disconnectingRPCConn) Done() <-chan struct{} {
	return c.conn.Done()
}

func NewDisconnectingRPCConnDialer(disconnect chan struct{}) rpcclient.Dialer {
	return func(ctx context.Context, addr string) rpcclient.BusConn {
		return NewDisconnectingRPCConn(ctx, addr, disconnect)
	}
}

func TestTxAbortedDuringNetworkPartition(t *testing.T) {
	env := yttest.New(t)

	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T) (tx yt.Tx, disconnect chan struct{})
	}{
		{
			name: "http",
			prepare: func(t *testing.T) (yt.Tx, chan struct{}) {
				yc := NewHTTPClient(t)

				drt := &disconnectingRoundTripper{
					disconnect: make(chan struct{}),
				}
				ctx := httpclient.WithRoundTripper(env.Ctx, drt)

				tx, err := yc.BeginTx(ctx, nil)
				require.NoError(t, err)

				return tx, drt.disconnect
			},
		},
		{
			name: "rpc",
			prepare: func(t *testing.T) (yt.Tx, chan struct{}) {
				yc := NewRPCClient(t)

				disconnect := make(chan struct{})
				dialer := NewDisconnectingRPCConnDialer(disconnect)
				ctx := rpcclient.WithDialer(env.Ctx, dialer)

				tx, err := yc.BeginTx(ctx, nil)
				require.NoError(t, err)

				return tx, disconnect
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, disconnect := tc.prepare(t)

			close(disconnect)

			select {
			case <-tx.Finished():
				return
			case <-time.After(time.Second * 30):
				t.Errorf("transaction is not aborted during network partition")
			}
		})
	}
}
