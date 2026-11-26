package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"

	"go.ytsaurus.tech/library/go/httputil/swaggerui"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/microservices/bulk_acl_checker/http_service_go/docs"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func RunServer(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("normal terminate"))
	ytClient := ytmsvc.MustNewYTClient(ytmsvc.Must(cmd.Flags().GetString("proxy")), ytmsvc.Must(cmd.Flags().GetString("token-env-variable")))
	Cache = InitCache(
		ytmsvc.Must(cmd.Flags().GetDuration("acl-cache-ttl")),
		ytmsvc.Must(cmd.Flags().GetDuration("banned-cache-ttl")),
	)
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
	var swaggerSpec map[string]any
	ytmsvc.Must0(json.Unmarshal(docs.SwaggerJSON, &swaggerSpec))
	publicAPI := func(r chi.Router) {
		r.Get("/swagger", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
		})
		r.Get("/swagger/*", serveSwaggerUI(swaggerSpec))
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

// Sets basePath of the specification to the version prefix of the request, such as /v1 or /testing.
func serveSwaggerUI(spec map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		basePath := "/" + chi.URLParam(r, "api_version")
		requestSpec := maps.Clone(spec)
		requestSpec["basePath"] = basePath
		specJSON, err := json.Marshal(requestSpec)
		if err != nil {
			ytmsvc.WriteError(w, r, yterrors.Err("could not encode the Swagger specification", err))
			return
		}
		ui := http.FileServer(swaggerui.NewFileSystem(swaggerui.WithJSONScheme(specJSON)))
		http.StripPrefix(path.Join(basePath, "swagger"), ui).ServeHTTP(w, r)
	}
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

// @Accept json
// @Produce json
// @Param request body bac_lib.CheckACLRequest true "Request body"
// @Success 200 {object} bac_lib.CheckACLResponse
// @Failure 400 {object} yterrors.Error
// @Router /check-acl [post]
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

// @Accept json
// @Produce json
// @Param request body bac_lib.ClickHouseDictRequest true "Newline-delimited JSON objects"
// @Success 200 {object} bac_lib.ClickHouseDictResponse "Newline-delimited JSON objects, one for each requested path"
// @Failure 400 {object} yterrors.Error
// @Router /clickhouse-dict [post]
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
