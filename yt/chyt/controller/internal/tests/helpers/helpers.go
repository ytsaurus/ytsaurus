package helpers

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/agent"
	"a.yandex-team.ru/yt/chyt/controller/internal/api"
	"a.yandex-team.ru/yt/chyt/controller/internal/httpserver"
	"a.yandex-team.ru/yt/chyt/controller/internal/monitoring"
	"a.yandex-team.ru/yt/chyt/controller/internal/sleep"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

const StrawberryRoot = ypath.Path("//tmp/strawberry")

func PrepareEnv(t *testing.T) *yttest.Env {
	env := yttest.New(t)

	_, err := env.YT.CreateNode(env.Ctx, StrawberryRoot, yt.NodeMap, &yt.CreateNodeOptions{
		Recursive:      true,
		IgnoreExisting: true,
	})
	require.NoError(t, err)

	_, err = env.YT.CreateObject(env.Ctx, yt.NodeAccessControlObjectNamespace, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": "test_family",
		},
		IgnoreExisting: true,
	})
	require.NoError(t, err)

	return env
}

func GenerateAlias() string {
	return "chyt" + guid.New().String()
}

type RequestClient struct {
	Endpoint string
	Proxy    string
	User     string

	httpClient *http.Client
	t          *testing.T
	env        *yttest.Env
}

type Response struct {
	StatusCode int
	Body       yson.RawValue
}

func (c *RequestClient) MakeRequest(httpMethod string, command string, params api.RequestParams) Response {
	body, err := yson.Marshal(params)
	require.NoError(c.t, err)

	c.env.L.Debug("making http api request", log.String("command", command), log.Any("params", params))

	req, err := http.NewRequest(httpMethod, c.Endpoint+"/"+command, bytes.NewReader(body))
	require.NoError(c.t, err)

	req.Header.Set("Content-Type", "application/yson")
	req.Header.Set("X-YT-TestUser", c.User)

	rsp, err := c.httpClient.Do(req)
	require.NoError(c.t, err)

	body, err = io.ReadAll(rsp.Body)
	require.NoError(c.t, err)

	c.env.L.Debug("http api request finished",
		log.String("command", command),
		log.Any("params", params),
		log.Int("status_code", rsp.StatusCode),
		log.String("response_body", string(body)))

	return Response{
		StatusCode: rsp.StatusCode,
		Body:       yson.RawValue(body),
	}
}

func (c *RequestClient) MakePostRequest(command string, params api.RequestParams) Response {
	return c.MakeRequest(http.MethodPost, c.Proxy+"/"+command, params)
}

func (c *RequestClient) MakeGetRequest(command string, params api.RequestParams) Response {
	return c.MakeRequest(http.MethodGet, command, params)
}

func PrepareClient(t *testing.T, env *yttest.Env, proxy string, server *httpserver.HTTPServer) *RequestClient {
	go server.Run()
	t.Cleanup(server.Stop)
	server.WaitReady()

	client := &RequestClient{
		Endpoint:   "http://" + server.RealAddress(),
		Proxy:      proxy,
		User:       "root",
		httpClient: &http.Client{},
		t:          t,
		env:        env,
	}

	return client
}

func PrepareAPI(t *testing.T) (*yttest.Env, *RequestClient) {
	env := PrepareEnv(t)

	proxy := os.Getenv("YT_PROXY")

	endpoint := ":0"

	c := api.HTTPAPIConfig{
		APIConfig: api.APIConfig{
			Family: "test_family",
			Stage:  "test_stage",
			Root:   StrawberryRoot,
		},
		Clusters:    []string{proxy},
		DisableAuth: true,
		Endpoint:    endpoint,
	}
	apiServer := api.NewServer(c, env.L.Logger())
	return env, PrepareClient(t, env, proxy, apiServer)
}

func abortAllOperations(t *testing.T, env *yttest.Env) {
	// TODO(max42): introduce some unique annotation and abort only such operations. This would allow
	// running this testsuite on real cluster.
	ops, err := yt.ListAllOperations(env.Ctx, env.YT, &yt.ListOperationsOptions{State: &yt.StateRunning})
	require.NoError(t, err)
	for _, op := range ops {
		err := env.YT.AbortOperation(env.Ctx, op.ID, &yt.AbortOperationOptions{})
		require.NoError(t, err)
	}
}

func CreateAgent(env *yttest.Env, stage string) *agent.Agent {
	l := log.With(env.L.Logger(), log.String("agent_stage", stage))

	passPeriod := yson.Duration(time.Millisecond * 400)
	config := &agent.Config{
		Root:       StrawberryRoot,
		PassPeriod: &passPeriod,
		Stage:      stage,
	}

	agent := agent.NewAgent(
		"test",
		env.YT,
		l,
		sleep.NewController(l.WithName("strawberry"),
			env.YT,
			StrawberryRoot,
			"test",
			nil),
		config)

	return agent
}

func PrepareAgent(t *testing.T) (*yttest.Env, *agent.Agent) {
	env, cancel := yttest.NewEnv(t)
	t.Cleanup(cancel)

	err := env.YT.RemoveNode(env.Ctx, StrawberryRoot, &yt.RemoveNodeOptions{Force: true, Recursive: true})
	require.NoError(t, err)
	_, err = env.YT.CreateNode(env.Ctx, StrawberryRoot, yt.NodeMap, &yt.CreateNodeOptions{
		Force:     true,
		Recursive: true,
		Attributes: map[string]interface{}{
			"controller_parameter": "default",
		},
	})
	require.NoError(t, err)

	nsPath := strawberry.AccessControlNamespacesPath.Child("sleep")
	err = env.YT.RemoveNode(env.Ctx, nsPath, &yt.RemoveNodeOptions{Force: true, Recursive: true})
	require.NoError(t, err)
	_, err = env.YT.CreateObject(env.Ctx, yt.NodeAccessControlObjectNamespace, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": "sleep",
		},
	})
	require.NoError(t, err)

	abortAllOperations(t, env)

	agent := CreateAgent(env, "default")

	return env, agent
}

type DummyLeader struct{}

func (a DummyLeader) IsLeader() bool {
	return true
}

func PrepareMonitoring(t *testing.T) (*yttest.Env, *agent.Agent, *RequestClient) {
	env, agent := PrepareAgent(t)
	proxy := os.Getenv("YT_PROXY")

	c := monitoring.HTTPMonitoringConfig{
		Clusters: []string{proxy},
		Endpoint: ":2223",
	}

	server := monitoring.NewServer(c, env.L.Logger(), DummyLeader{}, map[string]monitoring.Healther{
		proxy: agent,
	})
	return env, agent, PrepareClient(t, env, proxy, server)
}
