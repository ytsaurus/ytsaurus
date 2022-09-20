package monitoring

import (
	"net/http"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/httpserver"
)

type HTTPLeaderMonitoring struct {
	httpserver.HTTPResponser
	leader LeaderChecker
}

type HTTPHealthMonitoring struct {
	httpserver.HTTPResponser
	healther Healther
	leader   LeaderChecker
}

func NewHTTPLeaderMonitoring(leader LeaderChecker, l log.Logger) HTTPLeaderMonitoring {
	return HTTPLeaderMonitoring{
		HTTPResponser: httpserver.NewHTTPResponser(l),
		leader:        leader,
	}
}

func NewHTTPHealthMonitoring(healther Healther, leader LeaderChecker, l log.Logger) HTTPHealthMonitoring {
	return HTTPHealthMonitoring{
		HTTPResponser: httpserver.NewHTTPResponser(l),
		healther:      healther,
		leader:        leader,
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
	if a.leader.IsLeader() && a.healther.IsHealthy() {
		a.ReplyOK(w, struct{}{})
	} else {
		a.Reply(w, http.StatusServiceUnavailable, struct{}{})
	}
}
