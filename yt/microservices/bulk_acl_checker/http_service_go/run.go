package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"

	"go.ytsaurus.tech/yt/go/yt"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func RunServer(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("normal terminate"))
	ytClient := ytmsvc.MustNewYTClient(ytmsvc.Must(cmd.Flags().GetString("proxy")), ytmsvc.Must(cmd.Flags().GetString("token-env-variable")))
	go perClusterRunner(ctx, ytClient, cmd)
	port := ytmsvc.Must(cmd.Flags().GetUint16("port"))
	debugLogin := ytmsvc.Must(cmd.Flags().GetString("debug-login"))

	addr := fmt.Sprintf(":%v", port)
	var router http.Handler
	if debugLogin != "" {
		addr = fmt.Sprintf("127.0.0.1:%d", port)
		logger.Warnf("Authentication is disabled; using debug login %q on loopback", debugLogin)
		router = createDebugRouter(debugLogin)
	} else {
		router = GetRouterHandler(ytClient, cmd)
	}
	ytmsvc.Must0(http.ListenAndServe(addr, router))
}

func createDebugRouter(debugLogin string) http.Handler {
	accessChecker := &debugAccessChecker{login: debugLogin}
	whoamiHandler := func(http.ResponseWriter, *http.Request) (any, error) {
		return map[string]string{"user": debugLogin}, nil
	}
	return createRouter(
		GetInfoHandler(),
		whoamiHandler,
		GetServedClustersHandler(),
		createCheckACLHandler(accessChecker),
		GetClickHouseDictHandler(),
		GetLivenessHandler(),
		GetReadinessHandler(),
		GetDropCacheHandler(),
		GetMetricsHandler(),
	)
}

func createRouter(
	infoHandler ytmsvc.HTTPHandlerE,
	whoamiHandler ytmsvc.HTTPHandlerE,
	servedClustersHandler ytmsvc.HTTPHandlerE,
	checkACLHandler ytmsvc.HTTPHandlerE,
	clickHouseDictHandler http.HandlerFunc,
	livenessHandler ytmsvc.HTTPHandlerE,
	readinessHandler ytmsvc.HTTPHandlerE,
	dropCacheHandler ytmsvc.HTTPHandlerE,
	metricsHandler http.HandlerFunc,
) http.Handler {
	publicAPI := func(r chi.Router) {
		r.Post("/info", ytmsvc.FormatResponse(infoHandler))
		r.Post("/whoami", ytmsvc.FormatResponse(whoamiHandler))
		r.Post("/{call:served(-|_)clusters}", ytmsvc.FormatResponse(servedClustersHandler))
		r.Post("/{call:check(-|_)acl}", ytmsvc.FormatResponse(checkACLHandler))
		r.Post("/{call:clickhouse(-|_)dict}", clickHouseDictHandler)
	}

	r := chi.NewRouter()
	r.Use(GetRequestLoggerMiddleware)
	r.Get("/liveness", ytmsvc.FormatResponse(livenessHandler))
	r.Get("/readiness", ytmsvc.FormatResponse(readinessHandler))
	r.Post("/{call:drop(-|_)cache}", ytmsvc.FormatResponse(dropCacheHandler))
	r.Route("/{api_version}", publicAPI)
	r.Get("/metrics", metricsHandler)
	publicAPI(r)
	return r
}

type AccessChecker interface {
	CheckAccess(subject string, req *http.Request) (string, error)
}

type debugAccessChecker struct {
	login string
}

func (c *debugAccessChecker) CheckAccess(subject string, _ *http.Request) (string, error) {
	if subject != c.login {
		return "", fmt.Errorf("debug login %q cannot check subject %q", c.login, subject)
	}
	return "user/" + c.login, nil
}

func createCheckACLHandler(accessChecker AccessChecker) ytmsvc.HTTPHandlerE {
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
			ytmsvc.WriteError(w, httpReq, err)
			return
		}
		encoder := json.NewEncoder(w)
		for _, response := range result {
			err := encoder.Encode(response)
			if err != nil {
				ytmsvc.WriteError(w, httpReq, err)
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
