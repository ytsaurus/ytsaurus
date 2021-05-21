package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/go/yttest"
)

func TestLocks(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	prepare := func(t *testing.T) (name ypath.Path, tx0, tx1 yt.Tx) {
		name = env.TmpPath()
		_, err := env.YT.CreateNode(env.Ctx, name, yt.NodeMap, nil)
		require.NoError(t, err)

		tx0, err = env.YT.BeginTx(env.Ctx, nil)
		require.NoError(t, err)

		tx1, err = env.YT.BeginTx(env.Ctx, nil)
		require.NoError(t, err)

		return
	}

	t.Run("P", func(t *testing.T) {
		t.Run("Exclusive/Exclusive", func(t *testing.T) {
			name, tx0, tx1 := prepare(t)

			_, err := tx0.LockNode(env.Ctx, name, yt.LockExclusive, nil)
			require.NoError(t, err)

			_, err = tx1.LockNode(env.Ctx, name, yt.LockExclusive, nil)
			require.Error(t, err)
			require.True(t, yterrors.ContainsErrorCode(err, yterrors.ErrorCode(402)))
		})

		t.Run("Shared/Exclusive", func(t *testing.T) {
			name, tx0, tx1 := prepare(t)

			_, err := tx0.LockNode(env.Ctx, name, yt.LockExclusive, nil)
			require.NoError(t, err)

			_, err = tx1.LockNode(env.Ctx, name, yt.LockShared, nil)
			require.Error(t, err)
			require.True(t, yterrors.ContainsErrorCode(err, yterrors.ErrorCode(402)))
		})

		t.Run("Shared/Shared", func(t *testing.T) {
			name, tx0, tx1 := prepare(t)

			_, err := tx0.LockNode(env.Ctx, name, yt.LockShared, nil)
			require.NoError(t, err)

			_, err = tx1.LockNode(env.Ctx, name, yt.LockShared, nil)
			require.NoError(t, err)
		})

		t.Run("SharedAttr/SharedAttr", func(t *testing.T) {
			name, tx0, tx1 := prepare(t)

			attr := "type"
			options := &yt.LockNodeOptions{
				AttributeKey: &attr,
			}

			_, err := tx0.LockNode(env.Ctx, name, yt.LockShared, options)
			require.NoError(t, err)

			_, err = tx1.LockNode(env.Ctx, name, yt.LockShared, options)
			require.Error(t, err)
			require.True(t, yterrors.ContainsErrorCode(err, yterrors.ErrorCode(402)))
		})
	})
}

func TestSnapshotLock(t *testing.T) {
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
