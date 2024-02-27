package helpers

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/agent"
	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/chyt/controller/internal/httpserver"
	"go.ytsaurus.tech/yt/chyt/controller/internal/monitoring"
	"go.ytsaurus.tech/yt/chyt/controller/internal/sleep"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

type Env struct {
	*yttest.Env
	StrawberryRoot ypath.Path
}

func PrepareEnv(t *testing.T) *Env {
	env := yttest.New(t)

	strawberryRoot := env.TmpPath().Child("strawberry")

	_, err := env.YT.CreateNode(env.Ctx, strawberryRoot, yt.NodeMap, &yt.CreateNodeOptions{
		Recursive: true,
		Attributes: map[string]any{
			"controller_parameter": "default",
		},
	})
	require.NoError(t, err)

	_, err = env.YT.CreateObject(env.Ctx, yt.NodeAccessControlObjectNamespace, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": "sleep",
		},
		IgnoreExisting: true,
	})
	require.NoError(t, err)

	return &Env{env, strawberryRoot}
}

func GenerateAlias() string {
	return "chyt" + guid.New().String()
}

type RequestClient struct {
	Endpoint string
	Proxy    string
	User     string
	Env      *Env

	httpClient *http.Client
	t          *testing.T
}

type Response struct {
	StatusCode int
	Body       []byte
}

func (c *RequestClient) MakeRequest(httpMethod string, command string, params api.RequestParams, format api.FormatType) Response {
	body, err := api.Marshal(params, format)
	require.NoError(c.t, err)

	c.Env.L.Debug("making http api request", log.String("command", command), log.Any("params", params))

	req, err := http.NewRequest(httpMethod, c.Endpoint+"/"+command, bytes.NewReader(body))
	require.NoError(c.t, err)

	req.Header.Set("Content-Type", fmt.Sprintf("application/%v", format))
	req.Header.Set("Accept", fmt.Sprintf("application/%v", format))
	req.Header.Set("X-YT-TestUser", c.User)

	rsp, err := c.httpClient.Do(req)
	require.NoError(c.t, err)

	body, err = io.ReadAll(rsp.Body)
	require.NoError(c.t, err)

	c.Env.L.Debug("http api request finished",
		log.String("command", command),
		log.Any("params", params),
		log.Int("status_code", rsp.StatusCode),
		log.String("response_body", string(body)))

	return Response{
		StatusCode: rsp.StatusCode,
		Body:       body,
	}
}

func (c *RequestClient) MakePostRequest(command string, params api.RequestParams) Response {
	return c.MakeRequest(http.MethodPost, c.Proxy+"/"+command, params, api.DefaultFormat)
}

func (c *RequestClient) MakePostRequestWithFormat(command string, params api.RequestParams, format api.FormatType) Response {
	return c.MakeRequest(http.MethodPost, c.Proxy+"/"+command, params, format)
}

func (c *RequestClient) MakeGetRequest(command string, params api.RequestParams) Response {
	return c.MakeRequest(http.MethodGet, command, params, api.DefaultFormat)
}

func (c *RequestClient) GetBriefInfo(alias string) strawberry.OpletBriefInfo {
	r := c.MakePostRequest("get_brief_info", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(c.t, http.StatusOK, r.StatusCode)

	var rsp struct {
		Result strawberry.OpletBriefInfo `yson:"result"`
	}
	require.NoError(c.t, yson.Unmarshal(r.Body, &rsp))
	return rsp.Result
}

func PrepareClient(t *testing.T, env *Env, proxy string, server *httpserver.HTTPServer) *RequestClient {
	go server.Run()
	t.Cleanup(server.Stop)
	server.WaitReady()

	client := &RequestClient{
		Endpoint:   "http://" + server.RealAddress(),
		Proxy:      proxy,
		User:       "root",
		Env:        env,
		httpClient: &http.Client{},
		t:          t,
	}

	return client
}

func PrepareAPI(t *testing.T) (*Env, *RequestClient) {
	env := PrepareEnv(t)

	proxy := os.Getenv("YT_PROXY")

	c := api.HTTPAPIConfig{
		BaseAPIConfig: api.APIConfig{
			ControllerFactories: map[string]strawberry.ControllerFactory{
				"sleep": strawberry.ControllerFactory{
					Ctor: sleep.NewController,
				},
			},
			ControllerMappings: map[string]string{
				"*": "sleep",
			},
		},
		ClusterInfos: []strawberry.AgentInfo{
			{
				StrawberryRoot: env.StrawberryRoot,
				Stage:          "test_stage",
				Proxy:          proxy,
				Family:         "sleep",
			},
		},
		LocationAliases: map[string][]string{
			proxy: []string{"test_location_alias"},
		},
		DisableAuth: true,
		Endpoint:    ":0",
	}
	apiServer := api.NewServer(c, env.L.Logger())
	return env, PrepareClient(t, env, proxy, apiServer)
}

func abortAllOperations(t *testing.T, env *Env) {
	// TODO(max42): introduce some unique annotation and abort only such operations. This would allow
	// running this testsuite on real cluster.
	ops, err := yt.ListAllOperations(env.Ctx, env.YT, &yt.ListOperationsOptions{State: &yt.StateRunning})
	require.NoError(t, err)
	for _, op := range ops {
		err := env.YT.AbortOperation(env.Ctx, op.ID, &yt.AbortOperationOptions{})
		require.NoError(t, err)
	}
}

func CreateAgent(env *Env, stage string) *agent.Agent {
	l := log.With(env.L.Logger(), log.String("agent_stage", stage))

	passPeriod := yson.Duration(time.Millisecond * 400)
	collectOpsPeriod := yson.Duration(time.Millisecond * 200)
	config := &agent.Config{
		Root:                    env.StrawberryRoot,
		PassPeriod:              &passPeriod,
		CollectOperationsPeriod: &collectOpsPeriod,
		Stage:                   stage,
	}

	agent := agent.NewAgent(
		"test",
		env.YT,
		l,
		sleep.NewController(l.WithName("strawberry"),
			env.YT,
			env.StrawberryRoot,
			"test",
			nil),
		config)

	return agent
}

func PrepareAgent(t *testing.T) (*Env, *agent.Agent) {
	env := PrepareEnv(t)

	abortAllOperations(t, env)

	agent := CreateAgent(env, "default")

	return env, agent
}

type DummyLeader struct{}

func (a DummyLeader) IsLeader() bool {
	return true
}

func PrepareMonitoring(t *testing.T) (*Env, *agent.Agent, *RequestClient) {
	env, agent := PrepareAgent(t)
	proxy := os.Getenv("YT_PROXY")

	c := monitoring.HTTPMonitoringConfig{
		Clusters:                     []string{proxy},
		Endpoint:                     ":2223",
		HealthStatusExpirationPeriod: time.Duration(time.Minute),
	}

	server := monitoring.NewServer(c, env.L.Logger(), DummyLeader{}, map[string]monitoring.Healther{
		proxy: agent,
	})
	return env, agent, PrepareClient(t, env, proxy, server)
}

func Wait(t *testing.T, predicate func() bool) {
	t.Helper()

	for i := 0; i < 100; i++ {
		if !predicate() {
			time.Sleep(300 * time.Millisecond)
		} else {
			return
		}
	}
	require.True(t, predicate())
}
