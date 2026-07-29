package integration

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
)

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

func scrapeSolomonHistogramTotal(t *testing.T, client *helpers.RequestClient, sensor string) (total int64, found bool) {
	rsp := client.MakeGetRequest("solomon", api.RequestParams{})
	require.Equal(t, http.StatusOK, rsp.StatusCode)

	var dump struct {
		Metrics []struct {
			Labels map[string]string `json:"labels"`
			Hist   *struct {
				Buckets []int64 `json:"buckets"`
				Inf     int64   `json:"inf"`
			} `json:"hist"`
		} `json:"metrics"`
	}
	require.NoError(t, json.Unmarshal(rsp.Body, &dump))

	for _, m := range dump.Metrics {
		if m.Labels["sensor"] == sensor && m.Hist != nil {
			total := m.Hist.Inf
			for _, count := range m.Hist.Buckets {
				total += count
			}
			return total, true
		}
	}
	return 0, false
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

func TestSolomonPassDurationSensors(t *testing.T) {
	env, agent, client := helpers.PrepareSolomonMonitoring(t)
	t.Cleanup(agent.Stop)

	total, found := scrapeSolomonHistogramTotal(t, client, "oplet_pass_duration_seconds")
	require.True(t, found)
	require.Equal(t, int64(0), total)

	createStrawberryOp(t, env, "monitoring_test2")
	agent.Start()
	waitAliases(t, env, []string{"monitoring_test2"})

	helpers.Wait(t, func() bool {
		total, _ := scrapeSolomonHistogramTotal(t, client, "oplet_pass_duration_seconds")
		return total > 0
	})
	helpers.Wait(t, func() bool {
		return scrapeSolomonSensors(t, client)["last_pass_duration_seconds"] > 0
	})
}
