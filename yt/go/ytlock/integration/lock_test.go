package lock_integration_test

import (
	"context"
	"testing"
	"time"

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

func isCtxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func TestLockAcquire(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	path := rangomPath()
	env.YT.CreateNode(env.Ctx, path, yt.NodeFile, &yt.CreateNodeOptions{})

	lock := ytlock.NewLock(env.Ctx, env.YT, path)
	ctx, err := lock.Acquire()
	require.Nil(t, err)

	time.Sleep(100 * time.Millisecond)
	if isCtxDone(ctx) {
		t.Fatalf("Failed to acquire lock")
	}
}

func TestConcurrentLocks(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	path := rangomPath()
	env.YT.CreateNode(env.Ctx, path, yt.NodeFile, &yt.CreateNodeOptions{})

	firstLock := ytlock.NewLock(env.Ctx, env.YT, path)
	firstCtx, err := firstLock.Acquire()
	defer firstLock.Release()
	require.Nil(t, err)
	require.Equal(t, isCtxDone(firstCtx), false, "First lock should be acuired")

	time.Sleep(100 * time.Millisecond)

	secondLock := ytlock.NewLock(env.Ctx, env.YT, path)
	_, err = secondLock.Acquire()
	defer secondLock.Release()
	require.NotNil(t, err)

	firstLock.Release()
	t.Logf("Released first lock")

	var secondCtx context.Context
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		secondCtx, err = secondLock.Acquire()
		if err == nil {
			return
		}
	}
	require.Nil(t, err)
	require.Equal(t, isCtxDone(secondCtx), false, "Second lock should be acuired")
}

func TestLockCtxTermination(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	parentCtx, parentCancel := context.WithCancel(env.Ctx)
	path := rangomPath()
	lock := ytlock.NewLock(parentCtx, env.YT, path)
	ctx, _ := lock.Acquire()
	parentCancel()

	require.Equal(t, isCtxDone(ctx), true, "Lock context should be terminated by parent context")
}
