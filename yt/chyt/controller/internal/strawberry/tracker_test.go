package strawberry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

var root = ypath.Path("//tmp/tracker_root")

func prepare(t *testing.T) (ctx context.Context, env *yttest.Env) {
	env, _ = yttest.NewEnv(t)
	ctx, cancel := context.WithTimeout(env.Ctx, time.Second*10)
	t.Cleanup(cancel)

	_, err := env.YT.CreateNode(ctx, root, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true, Force: true})
	require.NoError(t, err)
	return
}

// waitForPaths is a helper to validate tracking behavior. Assuming no change of nodes happens concurrently
// with its invocation, it reliably receives all events that are to appear and validates that all received
// paths are from expectedPaths and each path is received at least once.
func waitForPaths(t *testing.T, l log.Structured, ch <-chan []ypath.Path, expectedPaths []string) {
	allowedPaths := make(map[ypath.Path]struct{})
	appearedPaths := make(map[ypath.Path]struct{})
	for _, path := range expectedPaths {
		allowedPaths[ypath.Path(path)] = struct{}{}
	}

	emptyCount := 0
	for {
		paths := <-ch
		if len(paths) == 0 {
			l.Debug("extracted empty batch")
			emptyCount++
			if emptyCount == 2 {
				// Assuming the absence of concurrent changes, two consecutive empty batches guarantee that
				// no events will ever appear, so it is ok to stop listening.
				break
			}
			continue
		}
		emptyCount = 0

		for _, path := range paths {
			l.Debug("extracted path", log.String("path", string(path)))

			if _, ok := allowedPaths[path]; !ok {
				t.Fatalf("received path %v which is not expected", path)
			}
			appearedPaths[path] = struct{}{}
		}
	}

	for path := range allowedPaths {
		if _, ok := appearedPaths[path]; !ok {
			t.Fatalf("path %v did not appear", path)
		}
	}
}

func drain(t *testing.T, l log.Structured, ch <-chan []ypath.Path) {
	for {
		paths, ok := <-ch
		if !ok {
			l.Debug("channel closed")
			break
		} else if len(paths) == 0 {
			l.Debug("extracted empty batch")
		} else {
			for _, path := range paths {
				l.Debug("extracted", log.String("event", string(path)))
			}
		}
	}
}

func TestTrackerSimple(t *testing.T) {
	ctx, env := prepare(t)

	_, err := env.YT.CreateNode(ctx, root.Child("foo"), yt.NodeMap, nil)
	require.NoError(t, err)

	ch := TrackChildren(ctx, root, time.Millisecond*100, env.YT, env.L.Logger())
	waitForPaths(t, env.L, ch, []string{"", "/foo"})

	err = env.YT.SetNode(ctx, root.Child("foo").Attr("a"), 42, nil)
	require.NoError(t, err)
	waitForPaths(t, env.L, ch, []string{"/foo"})

	// Create a subtree transactionally.
	err = yt.ExecTx(ctx, env.YT, func(ctx context.Context, tx yt.Tx) (err error) {
		_, err = tx.CreateNode(ctx, root.Child("bar"), yt.NodeMap, nil)
		// While we are in transaction, no revision changes are expected.
		if err != nil {
			return
		}

		waitForPaths(t, env.L, ch, []string{})

		_, err = tx.CreateNode(ctx, root.JoinChild("bar", "qux"), yt.NodeMap, nil)
		if err != nil {
			return
		}
		_, err = tx.CreateNode(ctx, root.JoinChild("bar", "quux"), yt.NodeMap, nil)
		if err != nil {
			return
		}
		return
	}, nil)
	require.NoError(t, err)

	// Recall that child creation changes the revision of a parent.
	waitForPaths(t, env.L, ch, []string{"", "/bar", "/bar/qux", "/bar/quux"})

	err = env.YT.RemoveNode(ctx, root.Child("foo"), nil)
	require.NoError(t, err)

	waitForPaths(t, env.L, ch, []string{"", "/foo"})

	err = env.YT.SetNode(ctx, root.Child("bar").Attr("opaque"), true, nil)
	require.NoError(t, err)

	// When bar becomes opaque, we stop seeing its children, thus they are considered removed.
	// Also note that due to current implementation of opaque attribute, it is changed without locking
	// the node, thus "/bar" will not be emitted.
	waitForPaths(t, env.L, ch, []string{"/bar/qux", "/bar/quux"})

	err = env.YT.SetNode(ctx, root.Child("bar").Child("qux").Attr("baz"), 3.14, nil)
	require.NoError(t, err)
	// "/bar/qux" is hidden by opaque "/bar", so we do not see its change.
	waitForPaths(t, env.L, ch, []string{})
}

func TestTrackerCancellation(t *testing.T) {
	ctx, env := prepare(t)

	tctx, cancel := context.WithCancel(ctx)

	ch := TrackChildren(tctx, root, time.Millisecond*100, env.YT, env.L.Logger())

	// Make sure tracker performs at least several passes.
	time.Sleep(time.Millisecond * 300)

	cancel()
	drain(t, env.L, ch)
}
