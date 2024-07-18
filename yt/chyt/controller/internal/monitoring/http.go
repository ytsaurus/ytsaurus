package monitoring

import (
	"net/http"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/httpserver"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type HTTPLeaderMonitoring struct {
	httpserver.HTTPResponder
	leaderChecker LeaderChecker
}

type HTTPHealthMonitoring struct {
	httpserver.HTTPResponder
	healthChecker HealthChecker
	leaderChecker LeaderChecker
}

func NewHTTPLeaderMonitoring(leaderChecker LeaderChecker, l log.Logger) HTTPLeaderMonitoring {
	return HTTPLeaderMonitoring{
		HTTPResponder: httpserver.NewHTTPResponder(l),
		leaderChecker: leaderChecker,
	}
}

func NewHTTPHealthMonitoring(healthChecker HealthChecker, leaderChecker LeaderChecker, c HTTPMonitoringConfig, l log.Logger) HTTPHealthMonitoring {
	return HTTPHealthMonitoring{
		HTTPResponder: httpserver.NewHTTPResponder(l),
		healthChecker: healthChecker,
		leaderChecker: leaderChecker,
	}
}

func (a HTTPLeaderMonitoring) HandleIsLeader(w http.ResponseWriter, r *http.Request) {
	if a.leaderChecker.IsLeader() {
		a.ReplyOK(w, struct{}{})
	} else {
		a.Reply(w, http.StatusServiceUnavailable, struct{}{})
	}
}

func (a HTTPHealthMonitoring) HandleIsHealthy(w http.ResponseWriter, r *http.Request) {
	if !a.leaderChecker.IsLeader() {
		a.Reply(w, http.StatusServiceUnavailable, map[string]any{
			"error": yterrors.Err("not a leader"),
		})
		return
	}

	if healthErr := a.healthChecker.CheckHealth(); healthErr != nil {
		a.Reply(w, http.StatusServiceUnavailable, map[string]any{
			"error": yterrors.FromError(healthErr),
		})
		return
	}

	a.ReplyOK(w, struct{}{})
}
