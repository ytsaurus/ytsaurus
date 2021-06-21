package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yt/ytrpc"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/yttest"
	"github.com/stretchr/testify/require"
)

func TestAdminClient(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	group := "group-" + guid.New().String()
	_, err := env.YT.CreateObject(env.Ctx, yt.NodeGroup, &yt.CreateObjectOptions{
		Attributes: map[string]interface{}{
			"name": group,
		}})
	require.NoError(t, err)

	user := "user-" + guid.New().String()
	_, err = env.YT.CreateObject(env.Ctx, yt.NodeUser, &yt.CreateObjectOptions{
		Attributes: map[string]interface{}{
			"name": user,
		},
	})
	require.NoError(t, err)

	memberOf := func(t *testing.T, user, group string) bool {
		t.Helper()

		var groups []string
		err := env.YT.GetNode(env.Ctx, ypath.Path("//sys/users").Child(user).Attr("member_of"), &groups, nil)
		require.NoError(t, err)

		for _, g := range groups {
			if g == group {
				return true
			}
		}

		return false
	}

	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	for _, tc := range []struct {
		name       string
		makeClient func() (yt.AdminClient, error)
	}{
		{name: "http", makeClient: func() (yt.AdminClient, error) {
			return ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
		{name: "rpc", makeClient: func() (yt.AdminClient, error) {
			return ytrpc.NewAdminClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := tc.makeClient()
			require.NoError(t, err)

			t.Run("AddRemoveMember", func(t *testing.T) {
				t.Parallel()

				require.False(t, memberOf(t, user, group))

				err := client.AddMember(ctx, group, user, nil)
				require.NoError(t, err)
				require.True(t, memberOf(t, user, group))

				err = client.RemoveMember(ctx, group, user, nil)
				require.NoError(t, err)
				require.False(t, memberOf(t, user, group))
			})

		})
	}
}
