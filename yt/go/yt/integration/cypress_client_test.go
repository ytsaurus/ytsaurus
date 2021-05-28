package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yt/ytrpc"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/go/ytlog"
)

func TestCypressClient(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	for _, tc := range []struct {
		name       string
		makeClient func() (yt.CypressClient, error)
	}{
		{name: "http", makeClient: func() (yt.CypressClient, error) {
			return ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
		{name: "rpc", makeClient: func() (yt.CypressClient, error) {
			return ytrpc.NewCypressClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := tc.makeClient()
			require.NoError(t, err)

			t.Run("CreateNode", func(t *testing.T) {
				t.Parallel()

				p := tmpPath()

				id, err := client.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
					Recursive: true,
				})
				require.NoError(t, err)

				ok, err := client.NodeExists(ctx, p, nil)
				require.NoError(t, err)
				require.True(t, ok)

				ok, err = client.NodeExists(ctx, id.YPath(), nil)
				require.NoError(t, err)
				require.True(t, ok)
			})

			t.Run("NodeExists", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				ok, err := client.NodeExists(ctx, ypath.Path("/"), nil)
				require.NoError(t, err)
				require.True(t, ok)

				ok, err = client.NodeExists(ctx, tmpPath(), nil)
				require.NoError(t, err)
				require.False(t, ok)
			})

			t.Run("RemoveNode", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				root := tmpPath()

				err := client.RemoveNode(ctx, root, nil)
				require.Error(t, err)
				require.True(t, yterrors.ContainsErrorCode(err, yterrors.ErrorCode(500)))

				p := root.Child(guid.New().String())

				_, err = client.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
					Recursive: true,
				})
				require.NoError(t, err)

				err = client.RemoveNode(ctx, root, &yt.RemoveNodeOptions{
					Recursive: true,
					Force:     true,
				})
				require.NoError(t, err)

				ok, err := client.NodeExists(ctx, root, nil)
				require.NoError(t, err)
				require.False(t, ok)
			})

			t.Run("GetNode", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				var attrs struct {
					Account  string `yson:"account"`
					Revision uint64 `yson:"revision"`
				}

				err := client.GetNode(ctx, ypath.Path("//@"), &attrs, nil)
				require.NoError(t, err)

				require.NotEqual(t, "", attrs.Account)
				require.NotZero(t, attrs.Revision)
			})

			t.Run("SetNode", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				p := ypath.Path("/").Attr(guid.New().String())

				value := "Gopher!"
				err := client.SetNode(ctx, p, value, nil)
				require.NoError(t, err)

				var cypressValue string
				err = client.GetNode(ctx, p, &cypressValue, nil)
				require.NoError(t, err)
				require.Equal(t, value, cypressValue)
			})

			t.Run("ListNode", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				var list []struct {
					Owner string `yson:"owner,attr"`
					Name  string `yson:",value"`
				}

				err := client.ListNode(ctx, ypath.Path("/"), &list, &yt.ListNodeOptions{
					Attributes: []string{"owner"},
				})
				require.NoError(t, err)

				require.NotEmpty(t, list)
				for _, node := range list {
					require.NotEqual(t, "", node.Name)
					require.NotEqual(t, "", node.Owner)
				}
			})

			t.Run("MultisetAttributes", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				p := tmpPath()

				_, err := client.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
					Recursive: true,
				})
				require.NoError(t, err)

				attrs := map[string]interface{}{
					"first_attr":  "some_value",
					"second_attr": 42,
				}
				err = client.MultisetAttributes(ctx, p.Attrs(), attrs, nil)
				require.NoError(t, err)

				cypressAttrs := struct {
					FirstAttr  string `yson:"first_attr"`
					SecondAttr int    `yson:"second_attr"`
				}{}
				err = client.GetNode(ctx, p.Attrs(), &cypressAttrs, nil)
				require.NoError(t, err)

				require.Equal(t, attrs["first_attr"], cypressAttrs.FirstAttr)
				require.Equal(t, attrs["second_attr"], cypressAttrs.SecondAttr)
			})

			t.Run("CopyNode", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				p := tmpPath()
				dst := tmpPath()

				_, err := client.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
					Attributes: map[string]interface{}{"foo": "bar"},
					Recursive:  true,
				})
				require.NoError(t, err)

				_, err = client.CopyNode(ctx, p, dst, nil)
				require.NoError(t, err)

				ok, err := client.NodeExists(ctx, dst.Attr("foo"), nil)
				require.NoError(t, err)
				require.True(t, ok)
			})

			t.Run("MoveNode", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				p := tmpPath()
				dst := tmpPath()

				_, err := client.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
					Attributes: map[string]interface{}{"foo": "bar"},
					Recursive:  true,
				})
				require.NoError(t, err)

				_, err = client.MoveNode(ctx, p, dst, nil)
				require.NoError(t, err)

				ok, err := client.NodeExists(ctx, dst.Attr("foo"), nil)
				require.NoError(t, err)
				require.True(t, ok)

				ok, err = client.NodeExists(ctx, p, nil)
				require.NoError(t, err)
				require.False(t, ok)
			})

			t.Run("LinkNode", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				p := tmpPath()
				link := tmpPath()

				_, err := client.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
					Recursive: true,
				})
				require.NoError(t, err)

				_, err = client.LinkNode(ctx, p, link, nil)
				require.NoError(t, err)

				var typ yt.NodeType
				err = client.GetNode(ctx, link.SuppressSymlink().Attr("type"), &typ, nil)
				require.NoError(t, err)
				require.Equal(t, yt.NodeLink, typ)
			})

			t.Run("CreateObject", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				account := guid.New().String()
				_, err := client.CreateObject(ctx, yt.NodeAccount, &yt.CreateObjectOptions{
					Attributes: map[string]interface{}{
						"name": account,
					},
				})
				require.NoError(t, err)

				p := ypath.Path("//sys/accounts").Child(account)
				ok, err := client.NodeExists(ctx, p, nil)
				require.NoError(t, err)
				require.True(t, ok)
			})

			t.Run("BinaryPath", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
				defer cancel()

				var value interface{}
				err := client.GetNode(ctx, ypath.Path("/привет"), &value, nil)
				require.Error(t, err)

				require.Contains(t, err.Error(), "привет")
			})
		})
	}
}
