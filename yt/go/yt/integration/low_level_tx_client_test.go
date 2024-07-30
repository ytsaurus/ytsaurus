package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func TestLowLevelTxClient(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "CommitTx", Test: suite.TestCommitTx},
		{Name: "PingAncestors", Test: suite.TestPingAncestors},
		{Name: "CommitTabletTx", Test: suite.TestCommitTabletTx},
		{Name: "AbortTx", Test: suite.TestAbortTx},
		{Name: "AbortTabletTx", Test: suite.TestAbortTabletTx},
	})
}

func (s *Suite) TestCommitTx(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	tx, err := yc.StartTx(ctx, nil)
	require.NoError(t, err)

	err = yc.PingTx(ctx, tx, nil)
	require.NoError(t, err)

	err = yc.CommitTx(ctx, tx, nil)
	require.NoError(t, err)

	err = yc.PingTx(ctx, tx, nil)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
}

func (s *Suite) TestPingAncestors(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	// duration should be more than default client pinger timeout
	duration := yt.DefaultTxPingPeriod + time.Second
	tx, err := yc.StartTx(ctx, &yt.StartTxOptions{Timeout: ptr.T(yson.Duration(duration))})
	require.NoError(t, err)

	txCli, err := yc.BeginTx(ctx, &yt.StartTxOptions{TransactionOptions: &yt.TransactionOptions{TransactionID: tx, PingAncestors: true}})
	require.NoError(t, err)

	time.Sleep(duration + time.Millisecond*100)

	err = yc.PingTx(ctx, tx, nil)
	require.NoError(t, err)

	err = yc.PingTx(ctx, txCli.ID(), nil)
	require.NoError(t, err)
}

func (s *Suite) TestCommitTabletTx(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	tx, err := yc.StartTabletTx(ctx, &yt.StartTabletTxOptions{
		Type:   yt.TxTypeTablet,
		Sticky: true,
	})
	require.NoError(t, err)

	err = yc.PingTx(ctx, tx, nil)
	require.NoError(t, err)

	err = yc.CommitTx(ctx, tx, nil)
	require.NoError(t, err)

	err = yc.PingTx(ctx, tx, nil)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
}

func (s *Suite) TestAbortTx(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	tx, err := yc.StartTx(ctx, nil)
	require.NoError(t, err)

	err = yc.AbortTx(ctx, tx, nil)
	require.NoError(t, err)

	err = yc.PingTx(ctx, tx, nil)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
}

func (s *Suite) TestAbortTabletTx(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	tx, err := yc.StartTabletTx(ctx, &yt.StartTabletTxOptions{
		Type:   yt.TxTypeTablet,
		Sticky: true,
	})
	require.NoError(t, err)

	err = yc.AbortTx(ctx, tx, nil)
	require.NoError(t, err)

	err = yc.PingTx(ctx, tx, nil)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
}
