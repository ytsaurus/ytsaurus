package integration

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func prepareResourceAccessAPI(t *testing.T) (*helpers.Env, *helpers.RequestClient) {
	t.Helper()
	env, c := helpers.PrepareSleepAPI(t)
	c.User = helpers.GenerateAlias()
	_, err := env.YT.CreateObject(env.Ctx, yt.NodeUser, &yt.CreateObjectOptions{
		Attributes: map[string]any{"name": c.User},
	})
	require.NoError(t, err)
	return env, c
}

func resourceUseACL(user string, allowed bool) []yt.ACE {
	action := yt.ActionDeny
	if allowed {
		action = yt.ActionAllow
	}
	return []yt.ACE{{Action: action, Subjects: []string{user}, Permissions: []yt.Permission{yt.PermissionUse}}}
}

func prepareDiskAccountAccessAPI(t *testing.T) (*helpers.Env, *helpers.RequestClient, string, string) {
	t.Helper()
	env, c := prepareResourceAccessAPI(t)
	allowedAccount, deniedAccount := helpers.GenerateAlias(), helpers.GenerateAlias()
	for _, account := range []string{allowedAccount, deniedAccount} {
		_, err := env.YT.CreateObject(env.Ctx, yt.NodeAccount, &yt.CreateObjectOptions{
			Attributes: map[string]any{
				"name": account,
				"acl":  resourceUseACL(c.User, account == allowedAccount),
			},
		})
		require.NoError(t, err)
	}
	return env, c, allowedAccount, deniedAccount
}

func preparePoolTreesAccessAPI(t *testing.T) (*helpers.Env, *helpers.RequestClient, string, string, string) {
	t.Helper()
	env, c := prepareResourceAccessAPI(t)
	pool := helpers.GenerateAlias()
	allowedTree, deniedTree := helpers.GenerateAlias(), helpers.GenerateAlias()
	for _, tree := range []string{allowedTree, deniedTree} {
		_, err := env.YT.CreateObject(env.Ctx, yt.NodeSchedulerPoolTree, &yt.CreateObjectOptions{
			Attributes: map[string]any{
				"name":   tree,
				"config": map[string]any{"node_tag_filter": tree},
			},
		})
		require.NoError(t, err)
		_, err = env.YT.CreateObject(env.Ctx, yt.NodeSchedulerPool, &yt.CreateObjectOptions{
			Attributes: map[string]any{
				"name":      pool,
				"pool_tree": tree,
				"acl":       resourceUseACL(c.User, tree == allowedTree),
			},
		})
		require.NoError(t, err)
	}
	return env, c, pool, allowedTree, deniedTree
}

func requireResourceAccessDenied(t *testing.T, r helpers.Response, message string) {
	t.Helper()
	require.Equal(t, http.StatusBadRequest, r.StatusCode, string(r.Body))
	var response struct {
		Message string `yson:"message"`
	}
	require.NoError(t, yson.Unmarshal(r.Body, &response))
	require.Equal(t, message, response.Message)
}

func requireRejectedResourceCreation(t *testing.T, env *helpers.Env, c *helpers.RequestClient, options map[string]any, message string) string {
	t.Helper()
	alias := helpers.GenerateAlias()
	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias, "speclet_options": options},
	})
	requireResourceAccessDenied(t, r, message)
	for _, path := range []ypath.Path{
		env.StrawberryRoot.Child(alias),
		env.StrawberryRoot.JoinChild(alias, "speclet"),
		strawberry.AccessControlNamespacesPath.JoinChild("sleep", alias),
	} {
		exists, err := env.YT.NodeExists(env.Ctx, path, nil)
		require.NoError(t, err)
		require.False(t, exists, "rejected creation must not leave %s", path)
	}
	return alias
}

func requireRejectedResourceEdit(t *testing.T, env *helpers.Env, c *helpers.RequestClient, alias, command string, params map[string]any, message string) {
	t.Helper()
	specletPath := env.StrawberryRoot.JoinChild(alias, "speclet")
	getOptions := &yt.GetNodeOptions{Attributes: []string{"value", "revision"}}
	var before, after map[string]any
	require.NoError(t, env.YT.GetNode(env.Ctx, specletPath.Attrs(), &before, getOptions))
	r := c.MakePostRequest(command, api.RequestParams{Params: params})
	requireResourceAccessDenied(t, r, message)
	require.NoError(t, env.YT.GetNode(env.Ctx, specletPath.Attrs(), &after, getOptions))
	require.Equal(t, before, after, "rejected edits must preserve both speclet contents and revision")
}

func TestHTTPAPIDiskAccountAccessOnCreation(t *testing.T) {
	t.Parallel()
	env, c, allowedAccount, deniedAccount := prepareDiskAccountAccessAPI(t)
	message := fmt.Sprintf("Use permission to account %q denied for user %q", deniedAccount, c.User)
	alias := requireRejectedResourceCreation(t, env, c, map[string]any{
		"disk_request": map[string]any{"account": deniedAccount, "disk_space": int64(1024)},
	}, message)
	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"speclet_options": map[string]any{
				"disk_request": map[string]any{"account": allowedAccount, "disk_space": int64(1024)},
			},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode, string(r.Body))
}

func TestHTTPAPIDiskAccountAccessOnModification(t *testing.T) {
	t.Parallel()
	env, c, allowedAccount, deniedAccount := prepareDiskAccountAccessAPI(t)
	message := fmt.Sprintf("Use permission to account %q denied for user %q", deniedAccount, c.User)
	for _, command := range []string{"set_option", "set_options", "edit_options", "set_speclet"} {
		for _, key := range []string{"disk_request", "disk_request/account"} {
			if command == "set_speclet" && key != "disk_request" {
				continue
			}
			t.Run(command+"/"+key, func(t *testing.T) {
				alias := helpers.GenerateAlias()
				r := c.MakePostRequest("create", api.RequestParams{
					Params: map[string]any{
						"alias": alias,
						"speclet_options": map[string]any{
							"disk_request": map[string]any{"account": allowedAccount, "disk_space": int64(1024)},
							"test_option":  int64(1),
						},
					},
				})
				require.Equal(t, http.StatusOK, r.StatusCode, string(r.Body))
				var value any = deniedAccount
				if key == "disk_request" {
					value = map[string]any{"account": deniedAccount, "disk_space": int64(2048)}
				}
				params := map[string]any{"alias": alias}
				options := map[string]any{key: value, "test_option": int64(2)}
				switch command {
				case "set_option":
					params["key"], params["value"] = key, value
				case "set_options":
					params["options"] = options
				case "edit_options":
					params["options_to_set"] = map[string]any{key: value}
					params["options_to_remove"] = []string{"test_option"}
				case "set_speclet":
					params["speclet"] = options
				}
				requireRejectedResourceEdit(t, env, c, alias, command, params, message)
			})
		}
	}
}

func TestHTTPAPIPoolTreesAccessOnCreation(t *testing.T) {
	t.Parallel()
	env, c, pool, allowedTree, deniedTree := preparePoolTreesAccessAPI(t)
	message := fmt.Sprintf("Use permission to pool %s in pool_tree %s denied for user %s", pool, deniedTree, c.User)
	for _, tc := range []struct {
		name  string
		trees []string
	}{
		{"denied_last", []string{allowedTree, deniedTree}},
		{"denied_first", []string{deniedTree, allowedTree}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alias := requireRejectedResourceCreation(t, env, c, map[string]any{"pool": pool, "pool_trees": tc.trees}, message)
			r := c.MakePostRequest("create", api.RequestParams{
				Params: map[string]any{
					"alias":           alias,
					"speclet_options": map[string]any{"pool": pool, "pool_trees": []string{allowedTree}},
				},
			})
			require.Equal(t, http.StatusOK, r.StatusCode, string(r.Body))
		})
	}
}

func TestHTTPAPIPoolTreesAccessOnModification(t *testing.T) {
	t.Parallel()
	env, c, pool, allowedTree, deniedTree := preparePoolTreesAccessAPI(t)
	message := fmt.Sprintf("Use permission to pool %s in pool_tree %s denied for user %s", pool, deniedTree, c.User)
	for _, command := range []string{"set_option", "set_options", "edit_options", "set_speclet"} {
		for _, key := range []string{"pool_trees", "pool_trees/0"} {
			if command == "set_speclet" && key != "pool_trees" {
				continue
			}
			t.Run(command+"/"+key, func(t *testing.T) {
				alias := helpers.GenerateAlias()
				r := c.MakePostRequest("create", api.RequestParams{
					Params: map[string]any{
						"alias": alias,
						"speclet_options": map[string]any{
							"pool": pool, "pool_trees": []string{allowedTree}, "test_option": int64(1),
						},
					},
				})
				require.Equal(t, http.StatusOK, r.StatusCode, string(r.Body))
				var value any = deniedTree
				if key == "pool_trees" {
					value = []string{allowedTree, deniedTree}
				}
				params := map[string]any{"alias": alias}
				options := map[string]any{key: value, "test_option": int64(2)}
				switch command {
				case "set_option":
					params["key"], params["value"] = key, value
				case "set_options":
					params["options"] = options
				case "edit_options":
					params["options_to_set"] = map[string]any{key: value}
					params["options_to_remove"] = []string{"test_option"}
				case "set_speclet":
					options["pool"] = pool
					params["speclet"] = options
				}
				requireRejectedResourceEdit(t, env, c, alias, command, params, message)
			})
		}
	}
}

func TestHTTPAPIActivationRequiresOnlyManagePermission(t *testing.T) {
	t.Parallel()
	env, c, account, _ := prepareDiskAccountAccessAPI(t)
	alias := helpers.GenerateAlias()
	r := c.MakePostRequest("create", api.RequestParams{Params: map[string]any{
		"alias": alias,
		"speclet_options": map[string]any{
			"disk_request": map[string]any{"account": account, "disk_space": int64(1024)},
		},
	}})
	require.Equal(t, http.StatusOK, r.StatusCode, string(r.Body))

	// Managing the clique does not require retaining use access to its resources.
	require.NoError(t, env.YT.SetNode(env.Ctx, ypath.Path("//sys/accounts").Child(account).Attr("acl"), resourceUseACL(c.User, false), nil))
	activePath := env.StrawberryRoot.JoinChild(alias, "speclet", "active")
	for _, active := range []bool{true, false} {
		for _, command := range []string{"start_stop", "set_option", "set_options", "edit_options"} {
			t.Run(fmt.Sprintf("%s/%t", command, active), func(t *testing.T) {
				require.NoError(t, env.YT.SetNode(env.Ctx, activePath, !active, nil))
				params := map[string]any{"alias": alias}
				switch command {
				case "start_stop":
					if active {
						command = "start"
					} else {
						command = "stop"
					}
				case "set_option":
					params["key"], params["value"] = "active", active
				case "set_options":
					params["options"] = map[string]any{"active": active}
				case "edit_options":
					params["options_to_set"] = map[string]any{"active": active}
					params["options_to_remove"] = []string{}
				}
				r := c.MakePostRequest(command, api.RequestParams{Params: params})
				require.Equal(t, http.StatusOK, r.StatusCode, string(r.Body))
				var stored bool
				require.NoError(t, env.YT.GetNode(env.Ctx, activePath, &stored, nil))
				require.Equal(t, active, stored)
			})
		}
	}
	// Changing resources in the same activation request still requires use access.
	requireRejectedResourceEdit(t, env, c, alias, "edit_options", map[string]any{
		"alias":             alias,
		"options_to_set":    map[string]any{"active": true, "disk_request/disk_space": int64(2048)},
		"options_to_remove": []string{},
	}, fmt.Sprintf("Use permission to account %q denied for user %q", account, c.User))
}
