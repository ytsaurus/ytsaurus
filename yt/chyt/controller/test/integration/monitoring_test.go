package integration

import (
	"encoding/json"
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

func TestAgentCoreMonitor(t *testing.T) {
	_, agent, client := helpers.PrepareMonitoring(t)
	t.Cleanup(agent.Stop)

	handler := "core_monitor/"
	rsp := client.MakeGetRequest(handler+client.Proxy, api.RequestParams{})
	require.Equal(t, http.StatusServiceUnavailable, rsp.StatusCode)

	agent.Start()
	defer agent.Stop()
	rsp = client.MakeGetRequest(handler+client.Proxy, api.RequestParams{})
	require.Equal(t, http.StatusOK, rsp.StatusCode)
}

func scrapeSolomonSensors(t *testing.T, client *helpers.RequestClient) map[string]float64 {
	rsp := client.MakeGetRequest("solomon", api.RequestParams{})
	require.Equal(t, http.StatusOK, rsp.StatusCode)

	var dump struct {
		Metrics []struct {
			Labels map[string]string `json:"labels"`
			Value  float64           `json:"value"`
		} `json:"metrics"`
	}
	require.NoError(t, json.Unmarshal(rsp.Body, &dump))

	sensors := make(map[string]float64)
	for _, m := range dump.Metrics {
		sensors[m.Labels["sensor"]] = m.Value
	}
	return sensors
}

func waitSolomonSensor(t *testing.T, client *helpers.RequestClient, sensor string, expected float64) {
	helpers.Wait(t, func() bool {
		value, ok := scrapeSolomonSensors(t, client)[sensor]
		return ok && value == expected
	})
}
func TestSolomonOpletCountSensors(t *testing.T) {
	env, agent, client := helpers.PrepareSolomonMonitoring(t)
	t.Cleanup(agent.Stop)

	sensors := scrapeSolomonSensors(t, client)
	require.Equal(t, float64(0), sensors["oplet_count"])
	require.Equal(t, float64(0), sensors["failed_oplet_count"])

	createStrawberryOp(t, env, "monitoring_test1")
	agent.Start()
	waitAliases(t, env, []string{"monitoring_test1"})

	waitSolomonSensor(t, client, "oplet_count", 1)

	require.Equal(t, float64(0), scrapeSolomonSensors(t, client)["pass_error_count"])

	agent.Stop()
	sensors = scrapeSolomonSensors(t, client)
	require.Equal(t, float64(0), sensors["oplet_count"])
	require.Equal(t, float64(0), sensors["failed_oplet_count"])
}
