package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestMountClient(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "Mount", Test: suite.TestMount},
		{Name: "Remount", Test: suite.TestRemount},
		{Name: "Freeze", Test: suite.TestFreeze},
		{Name: "Reshard", Test: suite.TestReshard},
	})
}

func (s *Suite) TestMount(t *testing.T, yc yt.Client) {
	t.Parallel()

	testSchema := schema.MustInfer(&testSchemaRow{})

	p := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, p, testSchema))

	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, p))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, p))

	require.NoError(t, migrate.UnmountAndWait(s.Ctx, yc, p))
	require.NoError(t, migrate.UnmountAndWait(s.Ctx, yc, p))
}

func (s *Suite) TestRemount(t *testing.T, yc yt.Client) {
	t.Parallel()

	testSchema := schema.MustInfer(&testSchemaRow{})

	p := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, p, testSchema))

	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, p))
	require.NoError(t, yc.RemountTable(s.Ctx, p, nil))
	require.NoError(t, waitTabletState(s.Ctx, yc, p, yt.TabletMounted))

	require.NoError(t, migrate.UnmountAndWait(s.Ctx, yc, p))
}

func (s *Suite) TestFreeze(t *testing.T, yc yt.Client) {
	t.Parallel()

	testSchema := schema.MustInfer(&testSchemaRow{})

	p := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, p, testSchema))

	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, p))

	require.NoError(t, migrate.FreezeAndWait(s.Ctx, yc, p))
	require.NoError(t, migrate.FreezeAndWait(s.Ctx, yc, p))

	require.NoError(t, migrate.UnfreezeAndWait(s.Ctx, yc, p))
	require.NoError(t, migrate.UnfreezeAndWait(s.Ctx, yc, p))

	require.NoError(t, migrate.UnmountAndWait(s.Ctx, yc, p))
}

func (s *Suite) TestReshard(t *testing.T, yc yt.Client) {
	t.Parallel()

	testSchema := schema.MustInfer(&testReshardRow{})

	p := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, p, testSchema))

	require.Error(t, yc.ReshardTable(s.Ctx, p, &yt.ReshardTableOptions{
		PivotKeys: [][]any{{"a"}},
	}), "first pivot key must match that of the first tablet in the resharded range")

	require.Error(t, yc.ReshardTable(s.Ctx, p, &yt.ReshardTableOptions{
		PivotKeys: [][]any{{}, {"b"}, {"a"}},
	}), "pivot keys must be strictly increasing")

	require.Error(t, yc.ReshardTable(s.Ctx, p, &yt.ReshardTableOptions{
		PivotKeys: []any{
			[]any{},
			testReshardRow{A: "c", B: 420},
		},
	}), "only slices could be used as pivot keys")

	require.NoError(t, yc.ReshardTable(s.Ctx, p, &yt.ReshardTableOptions{
		PivotKeys: []any{
			[]any{},
			[]any{"a"},
			[]any{"b", uint64(42)},
		},
	}))

	require.NoError(t, yc.ReshardTable(s.Ctx, p, &yt.ReshardTableOptions{
		TabletCount: ptr.Int(6),
	}))

	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, p))
	require.NoError(t, migrate.UnmountAndWait(s.Ctx, yc, p))
}

type testSchemaRow struct {
	A string `yson:",key"`
	B int
}

type testReshardRow struct {
	A string `yson:"a,key"`
	B uint64 `yson:"b,key"`
	C string `yson:"c,omitempty"`
}

func waitTabletState(ctx context.Context, yc yt.Client, path ypath.Path, state string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	return yt.PollMaster(ctx, yc, func() (stop bool, err error) {
		var currentState string
		err = yc.GetNode(ctx, path.Attr("tablet_state"), &currentState, nil)
		if err != nil {
			return
		}

		if currentState == state {
			stop = true
			return
		}

		return
	})
}
