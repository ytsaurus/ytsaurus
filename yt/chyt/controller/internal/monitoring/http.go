package monitoring

import (
	"net/http"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/httpserver"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type HTTPLeaderMonitoring struct {
	httpserver.HTTPResponser
	leader LeaderChecker
}

type HTTPHealthMonitoring struct {
	httpserver.HTTPResponser
	healther                     Healther
	leader                       LeaderChecker
	healthStatusExpirationPeriod time.Duration
}

func NewHTTPLeaderMonitoring(leader LeaderChecker, l log.Logger) HTTPLeaderMonitoring {
	return HTTPLeaderMonitoring{
		HTTPResponser: httpserver.NewHTTPResponser(l),
		leader:        leader,
	}
}

func NewHTTPHealthMonitoring(healther Healther, leader LeaderChecker, c HTTPMonitoringConfig, l log.Logger) HTTPHealthMonitoring {
	return HTTPHealthMonitoring{
		HTTPResponser:                httpserver.NewHTTPResponser(l),
		healther:                     healther,
		leader:                       leader,
		healthStatusExpirationPeriod: c.HealthStatusExpirationPeriod,
	}
}

func (a HTTPLeaderMonitoring) HandleIsLeader(w http.ResponseWriter, r *http.Request) {
	if a.leader.IsLeader() {
		a.ReplyOK(w, struct{}{})
	} else {
		a.Reply(w, http.StatusServiceUnavailable, struct{}{})
	}
}

func (a HTTPHealthMonitoring) HandleIsHealthy(w http.ResponseWriter, r *http.Request) {
	isLeader := a.leader.IsLeader()
	healthStatus := a.healther.GetHealthStatus()
	timeDelta := time.Since(healthStatus.ModificationTime)

	if isLeader && healthStatus.Err == nil && timeDelta <= a.healthStatusExpirationPeriod {
		a.ReplyOK(w, struct{}{})
		return
	}

	if !isLeader {
		a.Reply(w, http.StatusServiceUnavailable, struct{}{})
		return
	}

	if timeDelta > a.healthStatusExpirationPeriod {
		a.Reply(w, http.StatusServiceUnavailable, map[string]any{
			"error": yterrors.Err("health status has expired"),
		})
		return
	}

	if healthStatus.Err != nil {
		a.Reply(w, http.StatusServiceUnavailable, map[string]any{
			"error": yterrors.FromError(healthStatus.Err),
		})
		return
	}

	a.Reply(w, http.StatusServiceUnavailable, struct{}{})
}
