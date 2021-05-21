package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/go/yttest"
)

func TestErrors(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	badPath := ypath.Path("//foo/bar/zog")

	checkError := func(t *testing.T, err error) {
		require.Error(t, err)

		ytErr := err.(*yterrors.Error)
		require.Contains(t, ytErr.Attributes, "path")
		require.Equal(t, fmt.Sprint(ytErr.Attributes["path"]), badPath.String())
	}

	t.Run("SimpleRequest", func(t *testing.T) {
		err := env.YT.GetNode(env.Ctx, badPath, new(struct{}), nil)
		checkError(t, err)
	})

	t.Run("ReadRows", func(t *testing.T) {
		_, err := env.YT.ReadTable(env.Ctx, badPath, nil)
		checkError(t, err)
	})

	t.Run("WriteRows", func(t *testing.T) {
		w, err := env.YT.WriteTable(env.Ctx, badPath, nil)
		require.NoError(t, err)
		checkError(t, w.Commit())
	})

	t.Run("Read", func(t *testing.T) {
		_, err := env.YT.ReadFile(env.Ctx, badPath, nil)
		checkError(t, err)
	})

	t.Run("Write", func(t *testing.T) {
		f, err := env.YT.WriteFile(env.Ctx, badPath, nil)
		require.NoError(t, err)
		checkError(t, f.Close())
	})
}
