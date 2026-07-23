package monitoring

import (
	"github.com/go-chi/chi/v5"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/metrics/solomon"
	"go.ytsaurus.tech/library/go/yandex/solomon/reporters/puller/httppuller"
	"go.ytsaurus.tech/yt/chyt/controller/internal/httpserver"
)

func RegisterHTTPMonitoring(c HTTPMonitoringConfig, l log.Logger, leader LeaderChecker, healthers map[string]HealthChecker) chi.Router {
	r := chi.NewRouter()
	monitoring := NewHTTPLeaderMonitoring(leader, l)
	r.Get("/is_leader", monitoring.HandleIsLeader)
	for _, cluster := range c.Clusters {
		monitoring := NewHTTPHealthMonitoring(healthers[cluster], leader, c, l)
		r.Get("/is_healthy/"+cluster, monitoring.HandleIsHealthy)
		r.Get("/core_monitor/"+cluster, monitoring.HandleCoreMonitor)
	}
	return r
}

func RegisterHTTPSolomon(solomonRegistry *solomon.Registry) chi.Router {
	r := chi.NewRouter()
	if solomonRegistry == nil {
		return r
	}
	r.Mount("/solomon", httppuller.NewHandler(solomonRegistry, httppuller.WithSpack()))
	return r
}

func NewServer(c HTTPMonitoringConfig, l log.Logger, leader LeaderChecker, healthers map[string]HealthChecker) *httpserver.HTTPServer {
	return httpserver.New(c.Endpoint, RegisterHTTPMonitoring(c, l, leader, healthers))
}

func NewSolomonServer(endpoint string, solomonRegistry *solomon.Registry) *httpserver.HTTPServer {
	return httpserver.New(endpoint, RegisterHTTPSolomon(solomonRegistry))
}
