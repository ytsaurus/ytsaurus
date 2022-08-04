package helpers

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/api"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

const StrawberryRoot = ypath.Path("//tmp/strawberry")

func PrepareEnv(t *testing.T) *yttest.Env {
	env := yttest.New(t)

	_, err := env.YT.CreateNode(env.Ctx, StrawberryRoot, yt.NodeMap, &yt.CreateNodeOptions{Force: true, Recursive: true})
	require.NoError(t, err)

	return env
}

type APIClient struct {
	Endpoint string
	Proxy    string
	User     string

	httpClient *http.Client
	t          *testing.T
	env        *yttest.Env
}

type APIResponse struct {
	StatusCode int
	Body       yson.RawValue
}

func (c *APIClient) MakeRequest(command string, params api.RequestParams) APIResponse {
	body, err := yson.Marshal(params)
	require.NoError(c.t, err)

	c.env.L.Debug("making http api request", log.String("command", command), log.Any("params", params))

	req, err := http.NewRequest(http.MethodPost, c.Endpoint+"/"+c.Proxy+"/"+command, bytes.NewReader(body))
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

	return APIResponse{
		StatusCode: rsp.StatusCode,
		Body:       yson.RawValue(body),
	}
}

func PrepareAPI(t *testing.T) (*yttest.Env, *APIClient) {
	env := PrepareEnv(t)

	proxy := os.Getenv("YT_PROXY")

	c := api.HTTPServerConfig{
		HTTPAPIConfig: api.HTTPAPIConfig{
			APIConfig: api.APIConfig{
				Family: "test_family",
				Stage:  "test_stage",
				Root:   StrawberryRoot,
			},
			Clusters:    []string{proxy},
			DisableAuth: true,
		},
		Endpoint: ":0",
	}

	server := api.NewHTTPServer(c, env.L.Logger())
	go server.Run()
	t.Cleanup(server.Stop)
	server.WaitReady()

	client := &APIClient{
		Endpoint:   "http://" + server.RealAddress(),
		Proxy:      proxy,
		User:       "root",
		httpClient: &http.Client{},
		t:          t,
		env:        env,
	}

	return env, client
}
