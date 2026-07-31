package integration

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
)

func scrapeMetrics(t *testing.T, client *helpers.RequestClient) map[string]float64 {
	rsp := client.MakeGetRequest("metrics", api.RequestParams{})
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

func scrapeMetricsHistogramTotal(t *testing.T, client *helpers.RequestClient, sensor string) (total int64, found bool) {
	rsp := client.MakeGetRequest("metrics", api.RequestParams{})
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

func waitMetric(t *testing.T, client *helpers.RequestClient, sensor string, expected float64) {
	helpers.Wait(t, func() bool {
		value, ok := scrapeMetrics(t, client)[sensor]
		return ok && value == expected
	})
}

func TestMetricsOpletCountSensors(t *testing.T) {
	env, agent, client := helpers.PrepareMetricsMonitoring(t)
	t.Cleanup(agent.Stop)

	sensors := scrapeMetrics(t, client)
	require.Equal(t, float64(0), sensors["oplet_count"])
	require.Equal(t, float64(0), sensors["failed_oplet_count"])

	createStrawberryOp(t, env, "monitoring_test1")
	agent.Start()
	waitAliases(t, env, []string{"monitoring_test1"})

	waitMetric(t, client, "oplet_count", 1)

	require.Equal(t, float64(0), scrapeMetrics(t, client)["pass_error_count"])

	agent.Stop()
	sensors = scrapeMetrics(t, client)
	require.Equal(t, float64(0), sensors["oplet_count"])
	require.Equal(t, float64(0), sensors["failed_oplet_count"])
}

func TestMetricsPassDurationSensors(t *testing.T) {
	env, agent, client := helpers.PrepareMetricsMonitoring(t)
	t.Cleanup(agent.Stop)

	total, found := scrapeMetricsHistogramTotal(t, client, "oplet_pass_duration_seconds")
	require.True(t, found)
	require.Equal(t, int64(0), total)

	createStrawberryOp(t, env, "monitoring_test2")
	agent.Start()
	waitAliases(t, env, []string{"monitoring_test2"})

	helpers.Wait(t, func() bool {
		total, _ := scrapeMetricsHistogramTotal(t, client, "oplet_pass_duration_seconds")
		return total > 0
	})
	helpers.Wait(t, func() bool {
		return scrapeMetrics(t, client)["last_pass_duration_seconds"] > 0
	})
}
