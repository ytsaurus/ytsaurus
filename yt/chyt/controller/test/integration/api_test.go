package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestHTTPAPICreateAndRemove(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	ok, err := env.YT.NodeExists(env.Ctx, env.StrawberryRoot.Child(alias), nil)
	require.NoError(t, err)
	require.True(t, ok)

	var speclet map[string]any
	err = env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{
			"family": "sleep",
			"stage":  "test_stage",
		},
		speclet)

	// Alias already exists.
	r = c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusBadRequest, r.StatusCode)

	// Wrong arguments.
	r = c.MakePostRequest("create", api.RequestParams{})
	require.Equal(t, http.StatusBadRequest, r.StatusCode)
	r = c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{
			"alias": helpers.GenerateAlias(),
			"xxx":   "yyy",
		},
	})
	require.Equal(t, http.StatusBadRequest, r.StatusCode)

	r = c.MakePostRequest("remove", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	ok, err = env.YT.NodeExists(env.Ctx, env.StrawberryRoot.Child(alias), nil)
	require.NoError(t, err)
	require.False(t, ok)

	// Alias does not exist anymore.
	r = c.MakePostRequest("remove", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusBadRequest, r.StatusCode)

	// Alias with leading *.
	r = c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": "*" + alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	ok, err = env.YT.NodeExists(env.Ctx, env.StrawberryRoot.Child(alias), nil)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestHTTPAPIExists(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("exists", api.RequestParams{Params: map[string]any{"alias": alias}})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var result any

	err := yson.Unmarshal(r.Body, &result)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"result": false}, result)

	r = c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("exists", api.RequestParams{Params: map[string]any{"alias": alias}})
	require.Equal(t, http.StatusOK, r.StatusCode)

	err = yson.Unmarshal(r.Body, &result)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"result": true}, result)
}

func TestHTTPAPISetAndRemoveOption(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{Params: map[string]any{"alias": alias}})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("set_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "test_option",
			"value": 1234,
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var speclet map[string]any
	err := env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{
			"family":      "sleep",
			"stage":       "test_stage",
			"test_option": int64(1234),
		},
		speclet)

	r = c.MakePostRequest("set_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "test_dict/option",
			"value": "1234",
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	err = env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{
			"family":      "sleep",
			"stage":       "test_stage",
			"test_option": int64(1234),
			"test_dict": map[string]any{
				"option": "1234",
			},
		},
		speclet)

	r = c.MakePostRequest("remove_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "test_option",
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("remove_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "test_dict",
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	err = env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{
			"family": "sleep",
			"stage":  "test_stage",
		},
		speclet)

	r = c.MakePostRequest("remove_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "test_option",
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)
}

func TestHTTPAPIParseParams(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("set_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "test_dict/option",
			"value": "1234",
		},
		Unparsed: true,
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var speclet map[string]any
	err := env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{
			"family": "sleep",
			"stage":  "test_stage",
			"test_dict": map[string]any{
				"option": int64(1234),
			},
		},
		speclet)

	badAliases := []string{"9alias", "ali*s", "@alias"}
	for _, badAlias := range badAliases {
		r = c.MakePostRequest("create", api.RequestParams{
			Params: map[string]any{"alias": badAlias},
		})
		require.Equal(t, http.StatusBadRequest, r.StatusCode)
	}

	badOptions := []string{"@option", "op*tion", "very/@option"}
	for _, badOption := range badOptions {
		r = c.MakePostRequest("set_option", api.RequestParams{
			Params: map[string]any{
				"alias": alias,
				"key":   badOption,
				"value": "1234",
			},
			Unparsed: true,
		})
		require.Equal(t, http.StatusBadRequest, r.StatusCode)
	}
}

func TestHTTPAPISetPool(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	pool := guid.New().String()
	subpool := guid.New().String()
	thisPoolDoesNotExist := guid.New().String()

	_, err := env.YT.CreateObject(env.Ctx, yt.NodeSchedulerPool, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name":      pool,
			"pool_tree": "default",
		},
	})
	require.NoError(t, err)

	_, err = env.YT.CreateObject(env.Ctx, yt.NodeSchedulerPool, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name":        subpool,
			"pool_tree":   "default",
			"parent_name": pool,
		},
	})
	require.NoError(t, err)

	r = c.MakePostRequest("set_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "pool",
			"value": thisPoolDoesNotExist,
		},
	})
	require.Equal(t, http.StatusBadRequest, r.StatusCode)

	r = c.MakePostRequest("set_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "pool",
			"value": subpool,
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var speclet map[string]any
	err = env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{
			"family": "sleep",
			"stage":  "test_stage",
			"pool":   subpool,
		},
		speclet)
}

func TestHTTPAPIDescribeAndPing(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)

	rsp, err := http.Get(c.Endpoint + "/ping")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rsp.StatusCode)

	rsp, err = http.Get(c.Endpoint + "/describe")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rsp.StatusCode)

	body, err := io.ReadAll(rsp.Body)
	require.NoError(t, err)

	var description map[string]any
	err = yson.Unmarshal(body, &description)
	require.NoError(t, err)

	require.Equal(t, []any{c.Proxy, "test_location_alias"}, description["clusters"])

	// It's unlikely that the interface of the 'remove' command will be changed in the future,
	// so we rely on it in this test.
	deletePresent := false
	for _, anyCmd := range description["commands"].([]any) {
		cmd := anyCmd.(map[string]any)
		if reflect.DeepEqual(cmd["name"], "remove") {
			deletePresent = true
			params := cmd["parameters"].([]any)
			require.Equal(t, 1, len(params))
			param := params[0].(map[string]any)
			require.Equal(t, "alias", param["name"])
			require.Equal(t, true, param["required"])
		}
	}
	require.True(t, deletePresent)
}

func TestHTTPAPIList(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("list", api.RequestParams{})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var result map[string][]string

	err := yson.Unmarshal(r.Body, &result)
	require.NoError(t, err)
	require.Contains(t, result["result"], alias)

	r = c.MakePostRequest("list", api.RequestParams{
		Params: map[string]any{
			"attributes": []string{"creator", "test_option", "stage"},
		},
	})

	var resultWithAttrs map[string][]yson.ValueWithAttrs

	err = yson.Unmarshal(r.Body, &resultWithAttrs)
	require.NoError(t, err)
	require.Contains(t, resultWithAttrs["result"], yson.ValueWithAttrs{
		Value: alias,
		Attrs: map[string]any{
			"creator":     "root",
			"test_option": nil,
			"stage":       "test_stage",
		},
	})
}

func TestHTTPAPIGetBriefInfo(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("get_brief_info", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var result map[string]strawberry.OpletBriefInfo
	err := yson.Unmarshal(r.Body, &result)
	require.NoError(t, err)
	require.Equal(t, "", result["result"].HealthReason)

	r = c.MakePostRequest("set_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "active",
			"value": true,
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("get_brief_info", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	err = yson.Unmarshal(r.Body, &result)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(result["result"].HealthReason, "operation is pending restart"))
	require.Equal(t, result["result"].Creator, "root")
}

func TestHTTPAPIGetSpeclet(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("get_speclet", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var result map[string]any

	err := yson.Unmarshal(r.Body, &result)
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{
			"family": "sleep",
			"stage":  "test_stage",
		},
		result["result"])
}

func TestHTTPAPISetSpeclet(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	pool := guid.New().String()
	_, err := env.YT.CreateObject(env.Ctx, yt.NodeSchedulerPool, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name":      pool,
			"pool_tree": "default",
		},
	})
	require.NoError(t, err)

	r = c.MakePostRequest("set_speclet", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"speclet": map[string]any{
				"pool": pool,
			},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var speclet map[string]any
	err = env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{
			"family": "sleep",
			"stage":  "test_stage",
			"pool":   pool,
		},
		speclet)

	nonExistentPool := guid.New().String()
	r = c.MakePostRequest("set_speclet", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"speclet": map[string]any{
				"pool": nonExistentPool,
			},
		},
	})
	require.Equal(t, http.StatusBadRequest, r.StatusCode)
}

func TestHTTPAPIGetOption(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("get_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "stage",
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var result map[string]any
	err := yson.Unmarshal(r.Body, &result)
	require.NoError(t, err)
	require.Equal(t, "test_stage", result["result"])
}

func TestHTTPAPIStop(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("stop", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var result bool
	err := env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet", "active"), &result, nil)
	require.NoError(t, err)
	require.Equal(t, false, result)
}

func TestHTTPAPIStartStopUntracked(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("start", api.RequestParams{
		Params: map[string]any{
			"alias":     alias,
			"untracked": true,
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	op := getOp(t, env, alias)
	require.NotNil(t, op)
	require.False(t, op.State.IsFinished())
	require.Equal(t, "default", op.RuntimeParameters.Annotations["controller_parameter"])

	r = c.MakePostRequest("stop", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	op = getOp(t, env, alias)
	require.True(t, op == nil || op.State.IsFinished())

	err := env.YT.SetNode(env.Ctx, env.StrawberryRoot.Attr("controller_parameter"), "custom", nil)
	require.NoError(t, err)

	r = c.MakePostRequest("start", api.RequestParams{
		Params: map[string]any{
			"alias":     alias,
			"untracked": true,
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	op = getOp(t, env, alias)
	require.False(t, op.State.IsFinished())
	require.Equal(t, "custom", op.RuntimeParameters.Annotations["controller_parameter"])

	r = c.MakePostRequest("stop", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)
}

func TestHTTPAPISetOptions(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{Params: map[string]any{"alias": alias}})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("set_speclet", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"speclet": map[string]any{
				"option1": 1,
				"option2": "2",
				"option3": 3,
			},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("set_options", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"options": map[string]any{
				"option1": 10,
				"option2": "20",
			},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var speclet map[string]any
	err := env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	expected := map[string]any{
		"family":  "sleep",
		"stage":   "test_stage",
		"option1": int64(10),
		"option2": "20",
		"option3": int64(3),
	}
	require.Equal(t, expected, speclet)

	nonExistentPool := guid.New().String()
	r = c.MakePostRequest("set_options", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"options": map[string]any{
				"pool": nonExistentPool,
			},
		},
	})
	require.Equal(t, http.StatusBadRequest, r.StatusCode)

	err = env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	require.Equal(t, expected, speclet)
}

func stripOptions(optionGroups []strawberry.OptionGroupDescriptor) []strawberry.OptionGroupDescriptor {
	strippedGroups := make([]strawberry.OptionGroupDescriptor, len(optionGroups))
	for groupID, group := range optionGroups {
		strippedOptions := make([]strawberry.OptionDescriptor, len(group.Options))
		for optionID, option := range group.Options {
			strippedOptions[optionID] = strawberry.OptionDescriptor{
				Name:         option.Name,
				Type:         option.Type,
				CurrentValue: option.CurrentValue,
				DefaultValue: option.DefaultValue,
				Choices:      option.Choices,
			}
		}
		strippedGroups[groupID] = strawberry.OptionGroupDescriptor{
			Title:   group.Title,
			Options: strippedOptions,
			Hidden:  group.Hidden,
		}
	}
	return strippedGroups
}

func TestHTTPAPIDescribeOptions(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{Params: map[string]any{"alias": alias}})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("set_options", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"options": map[string]any{
				"preemption_mode": "graceful",
				"test_option":     10,
			},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("describe_options", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var rsp struct {
		Result []strawberry.OptionGroupDescriptor `yson:"result"`
	}
	err := yson.Unmarshal(r.Body, &rsp)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rsp.Result), 2)

	strippedResult := stripOptions(rsp.Result)

	require.Contains(t, strippedResult[0].Options, strawberry.OptionDescriptor{
		Name: "pool",
		Type: "pool",
	})
	require.Contains(t, strippedResult[len(strippedResult)-1].Options, strawberry.OptionDescriptor{
		Name:         "test_option",
		Type:         "int64",
		CurrentValue: int64(10),
	})
}

func TestHTTPAPIJSONFormat(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequestWithFormat(
		"create",
		api.RequestParams{Params: map[string]any{"alias": alias}},
		"json")
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequestWithFormat(
		"list",
		api.RequestParams{
			Params: map[string]any{
				"attributes": []string{"creator", "test_option"},
			},
		},
		"json",
	)
	require.Equal(t, http.StatusOK, r.StatusCode)

	var resultWithAttrs map[string][]api.AliasWithAttrs
	require.NoError(t, json.Unmarshal(r.Body, &resultWithAttrs))

	require.Contains(t, resultWithAttrs["result"], api.AliasWithAttrs{
		Alias: alias,
		Attrs: map[string]any{
			"creator":     "root",
			"test_option": nil,
		},
	})
}

func TestHTTPAPISetInt64ValueUsingJSONFormat(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{Params: map[string]any{"alias": alias}})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequestWithFormat(
		"set_option",
		api.RequestParams{
			Params: map[string]any{
				"alias": alias,
				"key":   "test_option",
				"value": 1,
			},
		},
		"json",
	)
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("get_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "test_option",
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var resultWithOption map[string]any
	err := yson.Unmarshal(r.Body, &resultWithOption)
	require.NoError(t, err)
	require.Equal(t, int64(1), resultWithOption["result"])
}

func checkAttrFromGetBriefInfoCommand(
	t *testing.T,
	c *helpers.RequestClient,
	alias string,
	attrName string,
	expectedValue any,
) {
	t.Helper()

	r := c.MakePostRequest("get_brief_info", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)
	var rsp struct {
		Result map[string]any `yson:"result"`
	}
	require.NoError(t, yson.Unmarshal(r.Body, &rsp))
	require.Equal(t, expectedValue, rsp.Result[attrName])
}

func checkAttrFromListCommand(
	t *testing.T,
	c *helpers.RequestClient,
	alias string,
	attrName string,
	expectedValue any,
) {
	t.Helper()

	r := c.MakePostRequest("list", api.RequestParams{
		Params: map[string]any{
			"attributes": []string{attrName},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)
	var rsp struct {
		Result []yson.ValueWithAttrs `yson:"result"`
	}
	require.NoError(t, yson.Unmarshal(r.Body, &rsp))
	require.Contains(t, rsp.Result, yson.ValueWithAttrs{
		Value: alias,
		Attrs: map[string]any{
			attrName: expectedValue,
		},
	})
}

func TestHTTPAPIStateAndStatus(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	pool := guid.New().String()
	_, err := env.YT.CreateObject(env.Ctx, yt.NodeSchedulerPool, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name":      pool,
			"pool_tree": "default",
		},
	})
	require.NoError(t, err)

	r = c.MakePostRequest("set_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "pool",
			"value": pool,
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("start", api.RequestParams{
		Params: map[string]any{
			"alias":     alias,
			"untracked": false,
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	checkAttrFromListCommand(t, c, alias, "state", "active")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "state", "active")
	// No agent to start op.
	checkAttrFromListCommand(t, c, alias, "health", "pending")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "health", "pending")

	r = c.MakePostRequest("stop", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	checkAttrFromListCommand(t, c, alias, "state", "inactive")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "state", "inactive")
	checkAttrFromListCommand(t, c, alias, "health", "good")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "health", "good")

	r = c.MakePostRequest("start", api.RequestParams{
		Params: map[string]any{
			"alias":     alias,
			"untracked": true,
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	checkAttrFromListCommand(t, c, alias, "state", "untracked")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "state", "untracked")

	helpers.Wait(t, func() bool {
		op := getOp(t, env, alias)
		return op != nil && op.State == yt.StateRunning
	})

	// TODO(dakovalkov): CHYT-1039
	// checkAttrFromListCommand(t, c, alias, "health", "good")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "health", "good")

	r = c.MakePostRequest("set_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "preemption_mode",
			"value": "graceful",
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	checkAttrFromListCommand(t, c, alias, "state", "untracked")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "state", "untracked")
	// TODO(dakovalkov): CHYT-1039
	// checkAttrFromListCommand(t, c, alias, "health", "failed")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "health", "failed")

	r = c.MakePostRequest("stop", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	checkAttrFromListCommand(t, c, alias, "state", "inactive")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "state", "inactive")
	checkAttrFromListCommand(t, c, alias, "health", "good")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "health", "good")

	r = c.MakePostRequest("remove_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "preemption_mode",
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("set_option", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"key":   "preemption_mode",
			"value": 1,
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	checkAttrFromListCommand(t, c, alias, "state", "inactive")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "state", "inactive")
	checkAttrFromListCommand(t, c, alias, "health", "failed")
	checkAttrFromGetBriefInfoCommand(t, c, alias, "health", "failed")
}

func TestHTTPAPICreateAndStart(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)

	pool := guid.New().String()
	nonExistingPool := guid.New().String()
	_, err := env.YT.CreateObject(env.Ctx, yt.NodeSchedulerPool, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name":      pool,
			"pool_tree": "default",
		},
	})
	require.NoError(t, err)

	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"speclet_options": map[string]any{
				"pool": nonExistingPool,
			},
		},
	})
	require.Equal(t, http.StatusBadRequest, r.StatusCode)

	r = c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"speclet_options": map[string]any{
				"active": true,
				"pool":   pool,
			},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var speclet map[string]any
	err = env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{
			"family": "sleep",
			"stage":  "test_stage",
			"active": true,
			"pool":   pool,
		},
		speclet)
}

func TestHTTPAPIListWorksIfSpecletIsIncorrect(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"speclet_options": map[string]any{
				"test_option":     "not_int",
				"network_project": 10,
			},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("list", api.RequestParams{
		Params: map[string]any{
			"attributes": []string{"test_option", "stage"},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var resultWithAttrs map[string][]yson.ValueWithAttrs
	require.NoError(t, yson.Unmarshal(r.Body, &resultWithAttrs))
	require.Contains(t, resultWithAttrs["result"], yson.ValueWithAttrs{
		Value: alias,
		Attrs: map[string]any{
			"test_option": nil,
			"stage":       "production",
		},
	})
}

func TestHTTPAPILocationAliases(t *testing.T) {
	t.Parallel()

	_, c := helpers.PrepareAPI(t)

	c.Proxy = "test_location_alias"
	r := c.MakePostRequest("list", api.RequestParams{})
	require.Equal(t, http.StatusOK, r.StatusCode)

	c.Proxy = "unknown_location"
	r = c.MakePostRequest("list", api.RequestParams{})
	require.Equal(t, http.StatusNotFound, r.StatusCode)
}

func TestHTTPAPIEditOptions(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	alias := helpers.GenerateAlias()

	r := c.MakePostRequest("create", api.RequestParams{Params: map[string]any{"alias": alias}})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("set_speclet", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"speclet": map[string]any{
				"option1": 1,
				"option2": "2",
				"option3": 3,
			},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	r = c.MakePostRequest("edit_options", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"options_to_set": map[string]any{
				"option1": 10,
				"option2": "20",
			},
			"options_to_remove": []string{"option3"},
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var speclet map[string]any
	err := env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	expected := map[string]any{
		"family":  "sleep",
		"stage":   "test_stage",
		"option1": int64(10),
		"option2": "20",
	}
	require.Equal(t, expected, speclet)

	nonExistentPool := guid.New().String()
	r = c.MakePostRequest("edit_options", api.RequestParams{
		Params: map[string]any{
			"alias": alias,
			"options": map[string]any{
				"pool": nonExistentPool,
			},
		},
	})
	require.Equal(t, http.StatusBadRequest, r.StatusCode)

	err = env.YT.GetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet"), &speclet, nil)
	require.NoError(t, err)
	require.Equal(t, expected, speclet)
}
