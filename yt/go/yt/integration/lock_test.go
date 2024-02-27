package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestLockClient(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "Exclusive/Exclusive", Test: suite.TestExclusiveExclusive},
		{Name: "Shared/Exclusive", Test: suite.TestSharedExclusive},
		{Name: "Shared/Shared", Test: suite.TestSharedShared},
		{Name: "SharedAttr/SharedAttr", Test: suite.TestSharedAttrSharedAttr},
	})
}

func (s *Suite) TestExclusiveExclusive(t *testing.T, yc yt.Client) {
	name, tx0, tx1 := s.prepareLockTest(t, yc)

	_, err := tx0.LockNode(s.Ctx, name, yt.LockExclusive, nil)
	require.NoError(t, err)

	_, err = tx1.LockNode(s.Ctx, name, yt.LockExclusive, nil)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeConcurrentTransactionLockConflict))
}

func (s *Suite) TestSharedExclusive(t *testing.T, yc yt.Client) {
	name, tx0, tx1 := s.prepareLockTest(t, yc)

	_, err := tx0.LockNode(s.Ctx, name, yt.LockExclusive, nil)
	require.NoError(t, err)

	_, err = tx1.LockNode(s.Ctx, name, yt.LockShared, nil)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeConcurrentTransactionLockConflict))
}

func (s *Suite) TestSharedShared(t *testing.T, yc yt.Client) {
	name, tx0, tx1 := s.prepareLockTest(t, yc)

	_, err := tx0.LockNode(s.Ctx, name, yt.LockShared, nil)
	require.NoError(t, err)

	_, err = tx1.LockNode(s.Ctx, name, yt.LockShared, nil)
	require.NoError(t, err)
}

func (s *Suite) TestSharedAttrSharedAttr(t *testing.T, yc yt.Client) {
	name, tx0, tx1 := s.prepareLockTest(t, yc)

	attr := "type"
	options := &yt.LockNodeOptions{
		AttributeKey: &attr,
	}

	_, err := tx0.LockNode(s.Ctx, name, yt.LockShared, options)
	require.NoError(t, err)

	_, err = tx1.LockNode(s.Ctx, name, yt.LockShared, options)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeConcurrentTransactionLockConflict))
}

func (s *Suite) prepareLockTest(t *testing.T, yc yt.Client) (name ypath.Path, tx0, tx1 yt.Tx) {
	name = tmpPath()
	_, err := s.YT.CreateNode(s.Ctx, name, yt.NodeMap, nil)
	require.NoError(t, err)

	tx0, err = yc.BeginTx(s.Ctx, nil)
	require.NoError(t, err)

	tx1, err = yc.BeginTx(s.Ctx, nil)
	require.NoError(t, err)

	return
}

func TestSnapshotLock(t *testing.T) { // todo rewrite as suite test after streaming methods are implemented in rpc client
	t.Parallel()

	env := yttest.New(t)

	tmpName := env.TmpPath()
	_, err := env.YT.CreateNode(env.Ctx, tmpName, yt.NodeTable, nil)
	require.NoError(t, err)

	require.NoError(t, env.UploadSlice(tmpName, []testRow{{Key: "a", Value: "1"}}))

	tx, err := env.YT.BeginTx(env.Ctx, nil)
	require.NoError(t, err)
	defer tx.Abort()

	lock, err := tx.LockNode(env.Ctx, tmpName, yt.LockSnapshot, nil)
	require.NoError(t, err)

	require.NoError(t, env.UploadSlice(tmpName, []testRow{{Key: "b", Value: "2"}}))

	var result []testRow
	require.NoError(t, yttest.DownloadSlice(env.Ctx, tx, lock.NodeID.YPath(), &result))
	require.Equal(t, []testRow{{Key: "a", Value: "1"}}, result)
}
