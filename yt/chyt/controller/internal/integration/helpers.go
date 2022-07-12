package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

const root = ypath.Path("//tmp/strawberry")

func prepareEnv(t *testing.T) *yttest.Env {
	env := yttest.New(t)

	_, err := env.YT.CreateNode(env.Ctx, root, yt.NodeMap, &yt.CreateNodeOptions{Force: true, Recursive: true})
	require.NoError(t, err)

	return env
}
