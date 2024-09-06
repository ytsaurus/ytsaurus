package ytwalk_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
	"go.ytsaurus.tech/yt/go/ytwalk"
)

type testNode struct {
	Value  any         `yson:",value"`
	Type   yt.NodeType `yson:"type,attr"`
	MyAttr string      `yson:"my_attr,attr"`
}

func TestWalk(t *testing.T) {
	env := yttest.New(t)

	_, err := env.YT.CreateNode(env.Ctx, ypath.Path("//home/ytwalk/foo/bar/zog"), yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"my_attr": "my_value",
		},
		Recursive: true,
	})
	require.NoError(t, err)

	require.NoError(t, env.YT.SetNode(env.Ctx, ypath.Path("//home/ytwalk/foo/@opaque"), true, nil))

	_, err = env.YT.CreateNode(env.Ctx, ypath.Path("//home/ytwalk/foo/string"), yt.NodeString, &yt.CreateNodeOptions{
		Recursive: true,
	})
	require.NoError(t, err)

	_, err = env.YT.CreateNode(env.Ctx, ypath.Path("//home/ytwalk/foo/bool"), yt.NodeBoolean, &yt.CreateNodeOptions{
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
			OnNode: func(path ypath.Path, node any) error {
				walkResult[path] = *(node.(*testNode))
				return nil
			},
		})
		require.NoError(t, err)

		require.Equal(t, walkResult, map[ypath.Path]testNode{
			"//home/ytwalk":             {Type: yt.NodeMap},
			"//home/ytwalk/foo":         {Type: yt.NodeMap},
			"//home/ytwalk/foo/string":  {Type: yt.NodeString, Value: ""},
			"//home/ytwalk/foo/bool":    {Type: yt.NodeBoolean, Value: false},
			"//home/ytwalk/foo/tree":    {Type: yt.NodeMap},
			"//home/ytwalk/foo/bar":     {Type: yt.NodeMap},
			"//home/ytwalk/foo/bar/zog": {Type: yt.NodeTable, MyAttr: "my_value"},
		})
	})

	t.Run("Skip", func(t *testing.T) {
		walkResult := map[ypath.Path]struct{}{}
		err = ytwalk.Do(env.Ctx, env.YT, &ytwalk.Walk{
			Root: ypath.Path("//home/ytwalk"),
			OnNode: func(path ypath.Path, node any) error {
				walkResult[path] = struct{}{}

				if path == "//home/ytwalk/foo/bar" {
					return ytwalk.ErrSkipSubtree
				}
				return nil
			},
		})
		require.NoError(t, err)

		require.Equal(t, walkResult, map[ypath.Path]struct{}{
			"//home/ytwalk":            {},
			"//home/ytwalk/foo":        {},
			"//home/ytwalk/foo/string": {},
			"//home/ytwalk/foo/bool":   {},
			"//home/ytwalk/foo/tree":   {},
			"//home/ytwalk/foo/bar":    {},
		})
	})
}
