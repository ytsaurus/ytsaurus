package lock_integration_test

import (
	"context"
	"testing"
	"time"

	"a.yandex-team.ru/yt/go/yterrors"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/ytlock"
	"a.yandex-team.ru/yt/go/yttest"

	"github.com/gofrs/uuid/v3"
	"github.com/stretchr/testify/require"
)

func rangomPath() ypath.Path {
	return ypath.Path("//tmp/yt-go-lock-test-" + uuid.Must(uuid.NewV4()).String())
}

func isDone(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

func TestLockAcquire(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	path := rangomPath()
	_, err := env.YT.CreateNode(env.Ctx, path, yt.NodeFile, &yt.CreateNodeOptions{})
	require.NoError(t, err)

	lock := ytlock.NewLock(env.YT, path)
	lost, err := lock.Acquire(env.Ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	require.False(t, isDone(lost), "Lock is lost")
}

func TestCreateIfMissing(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	path := rangomPath()
	lock := ytlock.NewLockOptions(env.YT, path, ytlock.Options{CreateIfMissing: true, LockMode: yt.LockExclusive})
	_, err := lock.Acquire(env.Ctx)
	require.NoError(t, err)
}

func TestLockReleaseAbortsTx(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	path := rangomPath()
	_, err := env.YT.CreateNode(env.Ctx, path, yt.NodeFile, &yt.CreateNodeOptions{})
	require.NoError(t, err)

	lock := ytlock.NewLock(env.YT, path)

	_, err = lock.Acquire(env.Ctx)
	require.NoError(t, err)

	err = lock.Release(env.Ctx)
	require.NoError(t, err)

	_, err = lock.Acquire(env.Ctx)
	require.NoError(t, err)
}

func TestConcurrentLocks(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	path := rangomPath()
	_, err := env.YT.CreateNode(env.Ctx, path, yt.NodeFile, &yt.CreateNodeOptions{})
	require.NoError(t, err)

	firstLock := ytlock.NewLock(env.YT, path)
	firstLost, err := firstLock.Acquire(env.Ctx)
	defer func() { _ = firstLock.Release(env.Ctx) }()
	require.Nil(t, err)
	require.False(t, isDone(firstLost), "First lock should be acquired")

	time.Sleep(100 * time.Millisecond)

	secondLock := ytlock.NewLock(env.YT, path)
	_, err = secondLock.Acquire(env.Ctx)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeConcurrentTransactionLockConflict))

	require.NoError(t, firstLock.Release(env.Ctx))
	t.Logf("Released first lock")

	secondLost, err := secondLock.Acquire(env.Ctx)
	require.NoError(t, err)
	require.False(t, isDone(secondLost), "Second lock should be acuired")
}

func TestLockCtxTermination(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	parentCtx, parentCancel := context.WithCancel(env.Ctx)

	path := rangomPath()
	_, err := env.YT.CreateNode(env.Ctx, path, yt.NodeFile, &yt.CreateNodeOptions{})
	require.NoError(t, err)

	lock := ytlock.NewLock(env.YT, path)
	lost, err := lock.Acquire(parentCtx)
	require.NoError(t, err)

	parentCancel()
	time.Sleep(time.Second)

	require.True(t, isDone(lost), "Lock context should be terminated by parent context")
}
