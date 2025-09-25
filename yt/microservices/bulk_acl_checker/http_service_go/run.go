package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"go.ytsaurus.tech/yt/go/yt"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
	lib "go.ytsaurus.tech/yt/microservices/lib/go"
)

func createRouter(
	infoHandler lib.HTTPHandlerE,
	whoamiHandler lib.HTTPHandlerE,
	servedClustersHandler lib.HTTPHandlerE,
	checkACLHandler lib.HTTPHandlerE,
	clickHouseDictHandler http.HandlerFunc,
	livenessHandler lib.HTTPHandlerE,
	readinessHandler lib.HTTPHandlerE,
	dropCacheHandler lib.HTTPHandlerE,
	metricsHandler http.HandlerFunc,
) http.Handler {
	publicAPI := func(r chi.Router) {
		r.Post("/info", lib.FormatResponse(infoHandler))
		r.Post("/whoami", lib.FormatResponse(whoamiHandler))
		r.Post("/{call:served(-|_)clusters}", lib.FormatResponse(servedClustersHandler))
		r.Post("/{call:check(-|_)acl}", lib.FormatResponse(checkACLHandler))
		r.Post("/{call:clickhouse(-|_)dict}", clickHouseDictHandler)
	}

	r := chi.NewRouter()
	r.Use(GetRequestLoggerMiddleware)
	r.Get("/liveness", lib.FormatResponse(livenessHandler))
	r.Get("/readiness", lib.FormatResponse(readinessHandler))
	r.Post("/{call:drop(-|_)cache}", lib.FormatResponse(dropCacheHandler))
	r.Route("/{api_version}", publicAPI)
	r.Get("/metrics", metricsHandler)
	publicAPI(r)
	return r
}

type AccessChecker interface {
	CheckAccess(subject string, req *http.Request) (string, error)
}

func createCheckACLHandler(accessChecker AccessChecker) lib.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		decoder := json.NewDecoder(req.Body)
		var reqBody bac_lib.CheckACLRequest
		err = decoder.Decode(&reqBody)
		if err != nil {
			return
		}

		var actor string
		actor, err = accessChecker.CheckAccess(reqBody.Subject, req)
		if err != nil {
			return
		}

		var checkResult []yt.SecurityAction
		checkResult, err = BulkCheckACL(req.Context(), reqBody, actor)
		if err != nil {
			return
		}

		result = bac_lib.CheckACLResponse{
			Actions: checkResult,
		}
		return
	}
}

func GetClickHouseDictHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, httpReq *http.Request) {
		result, err := ClickHouseCheckACL(w, httpReq)
		if err != nil {
			lib.WriteError(w, httpReq, err)
			return
		}
		encoder := json.NewEncoder(w)
		for _, response := range result {
			err := encoder.Encode(response)
			if err != nil {
				lib.WriteError(w, httpReq, err)
				return
			}
		}
	}
}

func GetMetricsHandler() http.HandlerFunc {
	host := getHostNameForMetrics()
	return func(w http.ResponseWriter, req *http.Request) {
		Cache.Mutex.Lock()
		defer Cache.Mutex.Unlock()
		now := time.Now().Unix()
		reg := getNewRegistry(map[string]string{"host": host})

		for cluster := range Cache.KnownClusters {
			cacheItem, ok := Cache.Clusters[cluster]
			if ok && cacheItem != nil {
				path := cacheItem.Version.String()
				pathArr := strings.Split(path, "/")
				timestamp, _ := strconv.ParseInt(strings.Split(pathArr[len(pathArr)-1], ":")[0], 10, 64)
				lag := now - timestamp
				subreg := reg.WithTags(map[string]string{"served_cluster": cluster})
				subreg.IntGauge("data_lag").Set(lag)
			}
		}
		for actor, metrics := range Metrics.Reset() {
			actorreg := reg.WithTags(map[string]string{"actor": actor})
			actorreg.IntGauge("checks_success").Set(metrics.SuccessChecks)
			actorreg.IntGauge("checks_fail").Set(metrics.FailChecks)
			actorreg.IntGauge("checks_cache_hit").Set(metrics.CacheHit)
		}

		streamRegistry(reg, w, req)
	}
}
