package ytwalk_test

import (
	"testing"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
	"a.yandex-team.ru/yt/go/ytwalk"
	"github.com/stretchr/testify/require"
)

type testNode struct {
	Type   yt.NodeType `yson:"type,attr"`
	MyAttr string      `yson:"my_attr,attr"`
}

func TestWalk(t *testing.T) {
	env := yttest.New(t)

	_, err := env.YT.CreateNode(env.Ctx, ypath.Path("//home/ytwalk/foo/bar/zog"), yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"my_attr": "my_value",
		},
		Recursive: true,
	})
	require.NoError(t, err)

	_, err = env.YT.CreateNode(env.Ctx, ypath.Path("//home/ytwalk/foo/tree"), yt.NodeMap, &yt.CreateNodeOptions{
		Recursive: true,
	})
	require.NoError(t, err)

	t.Run("Full", func(t *testing.T) {
		walkResult := map[ypath.Path]testNode{}
		err = ytwalk.Do(env.Ctx, env.YT, &ytwalk.Walk{
			Root:       ypath.Path("//home/ytwalk"),
			Attributes: []string{"type", "my_attr"},
			Node:       new(testNode),
			OnNode: func(path ypath.Path, node interface{}) error {
				walkResult[path] = *(node.(*testNode))
				return nil
			},
		})
		require.NoError(t, err)

		require.Equal(t, walkResult, map[ypath.Path]testNode{
			"//home/ytwalk":             {Type: yt.NodeMap},
			"//home/ytwalk/foo":         {Type: yt.NodeMap},
			"//home/ytwalk/foo/tree":    {Type: yt.NodeMap},
			"//home/ytwalk/foo/bar":     {Type: yt.NodeMap},
			"//home/ytwalk/foo/bar/zog": {Type: yt.NodeTable, MyAttr: "my_value"},
		})
	})

	t.Run("Skip", func(t *testing.T) {
		walkResult := map[ypath.Path]struct{}{}
		err = ytwalk.Do(env.Ctx, env.YT, &ytwalk.Walk{
			Root: ypath.Path("//home/ytwalk"),
			OnNode: func(path ypath.Path, node interface{}) error {
				walkResult[path] = struct{}{}

				if path == "//home/ytwalk/foo/bar" {
					return ytwalk.ErrSkipSubtree
				}
				return nil
			},
		})
		require.NoError(t, err)

		require.Equal(t, walkResult, map[ypath.Path]struct{}{
			"//home/ytwalk":          {},
			"//home/ytwalk/foo":      {},
			"//home/ytwalk/foo/tree": {},
			"//home/ytwalk/foo/bar":  {},
		})
	})
}
