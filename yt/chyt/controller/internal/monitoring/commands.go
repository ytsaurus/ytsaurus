package monitoring

import (
	"github.com/go-chi/chi/v5"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/httpserver"
)

func RegisterHTTPMonitoring(c HTTPMonitoringConfig, l log.Logger, leader LeaderChecker, healthers map[string]Healther) chi.Router {
	r := chi.NewRouter()
	monitoring := NewHTTPLeaderMonitoring(leader, l)
	r.Get("/is_leader", monitoring.HandleIsLeader)
	for _, cluster := range c.Clusters {
		monitoring := NewHTTPHealthMonitoring(healthers[cluster], leader, l)
		r.Get("/is_healthy/"+cluster, monitoring.HandleIsHealthy)
	}
	return r
}

func NewServer(c HTTPMonitoringConfig, l log.Logger, leader LeaderChecker, healthers map[string]Healther) *httpserver.HTTPServer {
	monitoringHandler := RegisterHTTPMonitoring(c, l, leader, healthers)
	return httpserver.New(c.Endpoint, monitoringHandler)
}
