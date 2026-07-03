package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

type distributedWriteRow struct {
	V int `yson:"v"`
}

// waitSignatureKeyPublished waits for the proxy to publish a signature key (done
// asynchronously after startup), without which distributed write sessions can't be signed.
func waitSignatureKeyPublished(t *testing.T, env *yttest.Env) {
	t.Helper()
	require.Eventually(t, func() bool {
		var owners []string
		err := env.YT.ListNode(env.Ctx, ypath.Path("//sys/public_keys/by_owner"), &owners, nil)
		return err == nil && len(owners) > 0
	}, time.Minute, time.Second)
}

func TestDistributedWrite(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)
	ctx := env.Ctx

	waitSignatureKeyPublished(t, env)

	path := env.TmpPath()
	_, err := env.YT.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{"schema": schema.MustInfer(&distributedWriteRow{})},
	})
	require.NoError(t, err)

	const cookieCount = 3
	const rowsPerCookie = 4

	cookieCountOpt := cookieCount
	session, err := env.YT.StartDistributedWriteSession(ctx, path, &yt.StartDistributedWriteSessionOptions{
		CookieCount: &cookieCountOpt,
	})
	require.NoError(t, err)
	require.Len(t, session.Cookies, cookieCount)

	results := make([]yt.WriteFragmentResult, 0, len(session.Cookies))
	for i, cookie := range session.Cookies {
		w, err := env.YT.WriteTableFragment(ctx, cookie, nil)
		require.NoError(t, err)
		for j := 0; j < rowsPerCookie; j++ {
			require.NoError(t, w.Write(distributedWriteRow{V: i*rowsPerCookie + j}))
		}
		require.NoError(t, w.Commit())
		results = append(results, w.Result())
	}

	require.NoError(t, env.YT.FinishDistributedWriteSession(ctx, session.Session, results, nil))

	var rowCount int
	require.NoError(t, env.YT.GetNode(ctx, path.Attr("row_count"), &rowCount, nil))
	require.Equal(t, cookieCount*rowsPerCookie, rowCount)
}

func TestDistributedWriteUnderTransaction(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)
	ctx := env.Ctx

	waitSignatureKeyPublished(t, env)

	path := env.TmpPath()
	_, err := env.YT.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{"schema": schema.MustInfer(&distributedWriteRow{})},
	})
	require.NoError(t, err)

	tx, err := env.YT.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Abort() }()

	const cookieCount = 3
	const rowsPerCookie = 4
	const totalRows = cookieCount * rowsPerCookie

	cookieCountOpt := cookieCount
	session, err := env.YT.StartDistributedWriteSession(ctx, path, &yt.StartDistributedWriteSessionOptions{
		CookieCount:        &cookieCountOpt,
		TransactionOptions: &yt.TransactionOptions{TransactionID: tx.ID()},
	})
	require.NoError(t, err)
	require.Len(t, session.Cookies, cookieCount)

	results := make([]yt.WriteFragmentResult, 0, len(session.Cookies))
	for i, cookie := range session.Cookies {
		w, err := env.YT.WriteTableFragment(ctx, cookie, nil)
		require.NoError(t, err)
		for j := 0; j < rowsPerCookie; j++ {
			require.NoError(t, w.Write(distributedWriteRow{V: i*rowsPerCookie + j}))
		}
		require.NoError(t, w.Commit())
		results = append(results, w.Result())
	}

	require.NoError(t, env.YT.FinishDistributedWriteSession(ctx, session.Session, results, nil))

	readValues := func(c yt.TableClient) []int {
		r, err := c.ReadTable(ctx, path, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		var values []int
		for r.Next() {
			var row distributedWriteRow
			require.NoError(t, r.Scan(&row))
			values = append(values, row.V)
		}
		require.NoError(t, r.Err())
		return values
	}

	expected := make([]int, totalRows)
	for i := range expected {
		expected[i] = i
	}

	require.ElementsMatch(t, expected, readValues(tx))
	// Not yet visible outside the transaction.
	require.Empty(t, readValues(env.YT))

	require.NoError(t, tx.Commit())

	require.ElementsMatch(t, expected, readValues(env.YT))
}
