package integration

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
)

func waitHealthCheck(t *testing.T, env *helpers.Env, client *helpers.RequestClient) {
	env.L.Debug("waiting for agent health check")
	for i := 0; i < 30; i++ {
		rsp := client.MakeGetRequest("is_healthy/"+client.Proxy, api.RequestParams{})
		if rsp.StatusCode == http.StatusOK {
			return
		}
		time.Sleep(time.Millisecond * 300)
	}
	env.L.Error("agent has not become healthy in time")
	t.FailNow()
}

func TestAgentIsHealthy(t *testing.T) {
	env, agent, client := helpers.PrepareMonitoring(t)
	t.Cleanup(agent.Stop)

	rsp := client.MakeGetRequest("is_healthy/"+client.Proxy, api.RequestParams{})
	require.Equal(t, http.StatusServiceUnavailable, rsp.StatusCode)

	agent.Start()
	defer agent.Stop()
	waitHealthCheck(t, env, client)
}
