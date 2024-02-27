package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/go/yttest"
)

func checkPathError(t *testing.T, err error, p ypath.Path) {
	t.Helper()

	require.Error(t, err)

	ytErr := err.(*yterrors.Error)
	require.Contains(t, ytErr.Attributes, "path")
	require.Equal(t, fmt.Sprint(ytErr.Attributes["path"]), p.String())
}

func TestErrors(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	badPath := ypath.Path("//foo/bar/zog")

	t.Run("SimpleRequest", func(t *testing.T) {
		err := env.YT.GetNode(env.Ctx, badPath, new(struct{}), nil)
		checkPathError(t, err, badPath)
	})

	t.Run("ReadRows", func(t *testing.T) {
		_, err := env.YT.ReadTable(env.Ctx, badPath, nil)
		checkPathError(t, err, badPath)
	})

	t.Run("WriteRows", func(t *testing.T) {
		w, err := env.YT.WriteTable(env.Ctx, badPath, nil)
		require.NoError(t, err)
		checkPathError(t, w.Commit(), badPath)
	})

	t.Run("Read", func(t *testing.T) {
		_, err := env.YT.ReadFile(env.Ctx, badPath, nil)
		checkPathError(t, err, badPath)
	})

	t.Run("Write", func(t *testing.T) {
		f, err := env.YT.WriteFile(env.Ctx, badPath, nil)
		if err != nil {
			checkPathError(t, err, badPath)
		} else {
			checkPathError(t, f.Close(), badPath)
		}
	})
}

func TestErrorInterceptor(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "GetNodeErrorInterceptor", Test: suite.TestGetNodeErrorInterceptor},
		{Name: "LookupErrorInterceptor", Test: suite.TestLookupErrorInterceptor},
	})
}

func (s *Suite) TestGetNodeErrorInterceptor(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	p := tmpPath()

	err := yc.GetNode(ctx, p, new(struct{}), nil)
	checkPathError(t, err, p)
}

func (s *Suite) TestLookupErrorInterceptor(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	p := tmpPath()

	keys := []any{
		&testKey{"bar"},
		&testKey{"foo"},
		&testKey{"baz"},
	}

	_, err := yc.LookupRows(ctx, p, keys, nil)
	checkPathError(t, err, p)
}
