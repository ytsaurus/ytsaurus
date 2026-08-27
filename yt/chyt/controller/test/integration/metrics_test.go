package integration

import (
	"encoding/json"
	"maps"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
)

type scrapedMetric struct {
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

func scrapeMetricSamples(t *testing.T, client *helpers.RequestClient) []scrapedMetric {
	rsp := client.MakeGetRequest("metrics", api.RequestParams{})
	require.Equal(t, http.StatusOK, rsp.StatusCode)

	var dump struct {
		Metrics []scrapedMetric `json:"metrics"`
	}
	require.NoError(t, json.Unmarshal(rsp.Body, &dump))
	return dump.Metrics
}

func scrapeMetrics(t *testing.T, client *helpers.RequestClient) map[string]float64 {
	sensors := make(map[string]float64)
	for _, m := range scrapeMetricSamples(t, client) {
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

func waitMetricSample(t *testing.T, client *helpers.RequestClient, labels map[string]string, expected float64) {
	helpers.Wait(t, func() bool {
		for _, metric := range scrapeMetricSamples(t, client) {
			if maps.Equal(metric.Labels, labels) && metric.Value == expected {
				return true
			}
		}
		return false
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

func TestMetricsControllerSensors(t *testing.T) {
	const alias = "controller_metrics_test"
	const otherAlias = "other_controller_metrics_test"

	var versions sync.Map
	var addOptionalTag atomic.Bool
	var invalidMetricCalls atomic.Int64
	var otherMetricValue atomic.Int64
	var omitOtherMetric atomic.Bool
	versions.Store(alias, "25.1")
	versions.Store(otherAlias, "24.8")
	otherMetricValue.Store(1)
	env, agent, client := helpers.PrepareMetricsMonitoringWithControllerMetrics(t, func(oplet *strawberry.Oplet) []strawberry.Metric {
		version, ok := versions.Load(oplet.Alias())
		if !ok {
			return nil
		}
		tags := map[string]string{"version": version.(string)}
		if addOptionalTag.Load() && oplet.Alias() == otherAlias {
			tags["optional"] = "present"
			invalidMetricCalls.Add(1)
		}
		metrics := []strawberry.Metric{{
			Name:  "chyt_server_version",
			Tags:  tags,
			Value: 1,
		}}
		if !omitOtherMetric.Load() || oplet.Alias() != alias {
			metrics = append(metrics, strawberry.Metric{
				Name:  "other_metric",
				Value: float64(otherMetricValue.Load()),
			})
		}
		return metrics
	})
	t.Cleanup(agent.Stop)

	createStrawberryOp(t, env, alias)
	createStrawberryOp(t, env, otherAlias)
	agent.Start()
	waitAliases(t, env, []string{alias, otherAlias})

	labels := map[string]string{
		"alias":   alias,
		"cluster": client.Proxy,
		"family":  "sleep",
		"sensor":  "controller.chyt_server_version",
		"stage":   "default",
		"version": "25.1",
	}
	waitMetricSample(t, client, labels, 1)
	otherLabels := maps.Clone(labels)
	otherLabels["alias"] = otherAlias
	otherLabels["version"] = "24.8"
	waitMetricSample(t, client, otherLabels, 1)
	otherMetricLabels := maps.Clone(labels)
	delete(otherMetricLabels, "version")
	otherMetricLabels["sensor"] = "controller.other_metric"
	waitMetricSample(t, client, otherMetricLabels, 1)
	otherAliasMetricLabels := maps.Clone(otherMetricLabels)
	otherAliasMetricLabels["alias"] = otherAlias
	waitMetricSample(t, client, otherAliasMetricLabels, 1)

	versions.Store(alias, "25.2")
	newLabels := maps.Clone(labels)
	newLabels["version"] = "25.2"
	waitMetricSample(t, client, newLabels, 1)
	waitMetricSample(t, client, labels, 0)
	waitMetricSample(t, client, otherLabels, 1)

	otherMetricValue.Store(2)
	omitOtherMetric.Store(true)
	addOptionalTag.Store(true)
	helpers.Wait(t, func() bool {
		return invalidMetricCalls.Load() >= 2
	})
	waitMetricSample(t, client, newLabels, 1)
	waitMetricSample(t, client, otherLabels, 1)
	waitMetricSample(t, client, otherMetricLabels, 0)
	waitMetricSample(t, client, otherAliasMetricLabels, 2)
	addOptionalTag.Store(false)
	omitOtherMetric.Store(false)

	versions.Delete(alias)
	waitMetricSample(t, client, newLabels, 0)
	waitMetricSample(t, client, otherLabels, 1)

	agent.Stop()
	helpers.Wait(t, func() bool {
		for _, metric := range scrapeMetricSamples(t, client) {
			if strings.HasPrefix(metric.Labels["sensor"], "controller.") {
				return false
			}
		}
		return true
	})
}
