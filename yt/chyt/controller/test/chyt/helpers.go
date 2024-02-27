package chyt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"go.ytsaurus.tech/library/go/test/yatest"
	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/chyt/controller/internal/chyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func setupBinaryDirectory(t *testing.T) (directory string) {
	t.Helper()

	trampolineBinary, err := yatest.BinaryPath("yt/chyt/trampoline/clickhouse-trampoline")
	require.NoError(t, err)

	chytBinary, err := yatest.BinaryPath("yt/chyt/server/bin/ytserver-clickhouse")
	require.NoError(t, err)

	wd, err := os.Getwd()
	require.NoError(t, err)

	binaryDir := fmt.Sprintf("%v/%v", wd, "chyt_binaries")
	require.NoError(t, os.Mkdir(binaryDir, 0777))

	require.NoError(t, os.Symlink(trampolineBinary, fmt.Sprintf("%v/%v", binaryDir, "clickhouse-trampoline")))
	require.NoError(t, os.Symlink(chytBinary, fmt.Sprintf("%v/%v", binaryDir, "ytserver-clickhouse")))

	return binaryDir
}

func getCHYTConfig(t *testing.T, binaryDirectory string) *helpers.Config {
	t.Helper()

	ctlConfig := yson.RawValue(fmt.Sprintf("{log_rotation_mode=disabled;local_binaries_dir=\"%v\";}", binaryDirectory))

	return &helpers.Config{
		Family:           "chyt",
		ControllerConfig: ctlConfig,
		ControllerFactory: strawberry.ControllerFactory{
			Ctor:   chyt.NewController,
			Config: ctlConfig,
		},
		ClusterInitializerFactory: chyt.NewClusterInitializer,
	}
}

func getDefaultSpeclet() map[string]any {
	return map[string]any{
		"instance_count": 1,
		"instance_cpu":   1,
		"instance_memory": map[string]any{
			"clickhouse":         2 * 1024 * 1024 * 1024,
			"chunk_meta_cache":   0,
			"compressed_cache":   0,
			"uncompressed_cache": 0,
			"reader":             1024 * 1024 * 1024,
		},
		"yt_config": map[string]any{
			"health_checker": map[string]any{
				"queries": []string{},
			},
		},
		"enable_geodata": false,
	}
}

type chytEnv struct {
	ctlClient  *helpers.RequestClient
	httpClient *http.Client
	ytEnv      *yttest.Env
	t          *testing.T
}

func newCHYTEnv(t *testing.T) (env *chytEnv, teardownCb func(t *testing.T)) {
	if os.Getenv("YT_PROXY") == "" {
		t.Skip("Skipping testing as there is no local yt.")
	}

	binaryDirectory := setupBinaryDirectory(t)
	config := getCHYTConfig(t, binaryDirectory)
	ctlClient, strawberryTeardownCb := helpers.PrepareController(t, config)
	env = &chytEnv{
		ctlClient:  ctlClient,
		ytEnv:      ctlClient.Env.Env,
		httpClient: &http.Client{},
		t:          t,
	}
	teardownCb = func(t *testing.T) {
		strawberryTeardownCb(t)
		require.NoError(t, os.RemoveAll(binaryDirectory))
	}
	return
}

func runCHYTClique(env *chytEnv, alias string, specletPatch map[string]any) (teardownCb func()) {
	speclet := getDefaultSpeclet()
	pool := createPool(env)
	speclet["pool"] = pool
	speclet["active"] = true
	for key, value := range specletPatch {
		speclet[key] = value
	}

	rsp := env.ctlClient.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{
			"alias":           alias,
			"speclet_options": speclet,
		},
	})
	require.Equal(env.t, http.StatusOK, rsp.StatusCode)

	helpers.Wait(env.t, func() bool {
		rsp := makeQueryWithFullResponse(env, "select 1", alias)
		if rsp.StatusCode != http.StatusOK {
			return false
		}

		var data []map[string]int
		require.NoError(env.t, json.Unmarshal(rsp.Data, &data))
		return reflect.DeepEqual(data, []map[string]int{
			map[string]int{"1": 1},
		})
	})

	return func() {
		teardownCHYTClique(env, alias)
	}
}

func teardownCHYTClique(env *chytEnv, alias string) {
	rsp := env.ctlClient.MakePostRequest("stop", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(env.t, http.StatusOK, rsp.StatusCode)

	helpers.Wait(env.t, func() bool {
		rsp := env.ctlClient.MakePostRequest("get_brief_info", api.RequestParams{
			Params: map[string]any{"alias": alias},
		})
		require.Equal(env.t, http.StatusOK, rsp.StatusCode)

		var result map[string]strawberry.OpletBriefInfo
		require.NoError(env.t, yson.Unmarshal(rsp.Body, &result))
		return result["result"].YTOperation.State.IsFinished()
	})

	rsp = env.ctlClient.MakePostRequest("remove", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(env.t, http.StatusOK, rsp.StatusCode)
}

type Response struct {
	StatusCode int
	Data       json.RawMessage
}

func makeQueryWithFullResponse(env *chytEnv, query string, alias string) Response {
	queryTypesWithOutput := []string{"describe", "select", "show", "exists", "explain", "with"}

	fields := strings.Fields(query)
	require.NotEmpty(env.t, fields)

	outputPresent := slices.Contains(queryTypesWithOutput, strings.ToLower(fields[0]))
	if outputPresent {
		query = query + " format JSON"
	}

	url := fmt.Sprintf("http://%v/query?database=*%v", os.Getenv("YT_PROXY"), alias)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(query)))
	require.NoError(env.t, err)

	req.Header.Set("X-YT-TestUser", env.ctlClient.User)

	rsp, err := env.httpClient.Do(req)
	require.NoError(env.t, err)

	body, err := io.ReadAll(rsp.Body)
	require.NoError(env.t, err)

	var data json.RawMessage
	if rsp.StatusCode == http.StatusOK && outputPresent {
		var result map[string]json.RawMessage
		require.NoError(env.t, json.Unmarshal(body, &result))

		data = result["data"]
	}

	return Response{
		StatusCode: rsp.StatusCode,
		Data:       data,
	}
}

func makeQuery(env *chytEnv, query string, alias string) json.RawMessage {
	rsp := makeQueryWithFullResponse(env, query, alias)
	require.Equal(env.t, http.StatusOK, rsp.StatusCode)
	return rsp.Data
}

func createPool(env *chytEnv) string {
	pool := guid.New().String()
	_, err := env.ytEnv.YT.CreateObject(env.ytEnv.Ctx, yt.NodeSchedulerPool, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name":      pool,
			"pool_tree": "default",
		},
	})
	require.NoError(env.t, err)

	return pool
}
