package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

const (
	QueryTrackerRowCountLimitStr = "10000"
)

var logger *zap.Logger

var ClustersVisibleRanges = make(map[string]VisibleTimeRangeResponse)

func GetLivenessHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		return ytmsvc.ResponseStatusOK, nil
	}
}

func GetReadinessHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		if len(ClustersVisibleRanges) == 0 {
			return nil, yterrors.Err("not ready")
		}
		return ytmsvc.ResponseStatusOK, nil
	}
}

func GetInfoHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		return
	}
}

func GetWhoamiHandler(authorizer *Authorizer) ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		var reqData struct {
			Cluster string `json:"cluster"`
		}
		if err := json.NewDecoder(req.Body).Decode(&reqData); err != nil {
			return nil, err
		}
		type Response struct {
			User string `json:"user"`
		}
		user, ticket, err := authorizer.getUser(req, reqData.Cluster)
		_ = ticket
		if err != nil {
			return nil, err
		}
		return Response{
			User: user.Login,
		}, nil
	}
}

func GetServedClustersHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		response := ServedClustersResponse{}
		for cluster := range ClustersVisibleRanges {
			response.Clusters = append(response.Clusters, cluster)
		}
		return response, nil
	}
}

func GetMetricsHandler() http.HandlerFunc {
	host := ytmsvc.GetHostNameForMetrics()
	return func(w http.ResponseWriter, req *http.Request) {
		now := time.Now().Unix()
		reg := ytmsvc.GetNewRegistry(map[string]string{"host": host})
		for cluster, visibleRange := range ClustersVisibleRanges {
			subreg := reg.WithTags(map[string]string{"served_cluster": cluster})
			subreg.IntGauge("data_lag").Set(now - visibleRange.Latests)
		}
		ytmsvc.StreamRegistry(reg, w, req)
	}
}

func GetRequestLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := ytmsvc.GetRequestSomeID(r)
		logger.Debugf("Start processing query: %s %s", id, r.URL)
		next.ServeHTTP(w, r)
		logger.Debugf("Finish processing query: %s %s", id, r.URL)
	})
}

func DoClickhouseHTTPRequestWithRedirectAndPreserveAuthorization(urlStr string, data []byte, ytTokenEnvVariable string) (result io.ReadCloser, err error) {
	httpReq, err := http.NewRequest("POST", urlStr, bytes.NewBuffer(data))
	if err != nil {
		return
	}
	httpReq.Header.Set("Authorization", "OAuth "+os.Getenv(ytTokenEnvVariable))
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		return
	}
	if httpResp.StatusCode == 307 {
		defer httpResp.Body.Close()
		var location *url.URL
		location, err = httpResp.Location()
		if err != nil {
			return
		}
		return DoClickhouseHTTPRequestWithRedirectAndPreserveAuthorization(location.String(), data, ytTokenEnvVariable)
	}

	result = httpResp.Body
	return
}

func DoClickhouseQuery[T any](chytCluster, chytAlias, query string, settings map[string]string, ytTokenEnvVariable string) (result []T, err error) {
	var params []string
	for k, v := range settings {
		param := fmt.Sprintf("%s=%s", k, url.QueryEscape(v))
		params = append(params, param)
	}
	params = append(params, "default_format=JSONEachRow")
	params = append(params, "output_format_json_quote_64bit_integers=0")
	paramsStr := strings.Join(params, "&")

	url := buildChytUrl(chytCluster, chytAlias, paramsStr)
	body, err := DoClickhouseHTTPRequestWithRedirectAndPreserveAuthorization(url, []byte(query), ytTokenEnvVariable)
	if err != nil {
		return
	}
	return ReadClickhouseResponse[T](body)
}

func GetQueryPriority() string {
	var priority = int32(time.Now().Unix() - 1700000000)
	return fmt.Sprintf("%d", priority)
}

func ReadClickhouseResponse[T any](input io.ReadCloser) (result []T, err error) {
	defer func() { _ = input.Close() }()
	result = []T{}
	decoder := json.NewDecoder(input)
	for {
		var record T
		err = decoder.Decode(&record)
		if err == io.EOF {
			err = nil
			return // normal
		}
		if err != nil {
			if len(result) == 0 {
				var data []byte
				data, err = io.ReadAll(decoder.Buffered())
				if err != nil {
					return // Can't read from ClickHouse
				}
				// ClickHouse return error in answer
				err = yterrors.Err(string(data))
				if strings.Contains(err.Error(), "No tables to read from") {
					err = yterrors.Err(
						"Data is not ready yet. Please try again later or select other dates.",
						err,
					)
				}
				return
			}
			// Empty ClickHouse answer
			return result, yterrors.Err("Empty ClickHouse answer")
		}
		result = append(result, record)
	}
}

func GetAccessLogHandler(chytCluster, chytAlias, snapshotRoot string, authorizer *Authorizer, ytTokenEnvVariable string) ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		request, err := ReadAccessLogRequest(req)
		if err != nil {
			return
		}
		user, _, err := authorizer.getUser(req, request.Cluster)
		if err != nil {
			return
		}
		if _, ok := ClustersVisibleRanges[request.Cluster]; !ok {
			err = fmt.Errorf("cluster '%s' not served", request.Cluster)
			return
		}
		query, settings, err := GetQuery(user.Login, request, snapshotRoot, 0, 0)
		if err != nil {
			return
		}
		data, err := DoClickhouseQuery[AccessRecord](chytCluster, chytAlias, query, settings, ytTokenEnvVariable)
		if err != nil {
			return
		}
		totalRowCount := uint32(len(data))
		if request.Pagination.Size > 0 {
			offset := request.Pagination.Index * request.Pagination.Size
			if totalRowCount < offset {
				data = []AccessRecord{}
			} else {
				remain := totalRowCount - offset
				if remain < request.Pagination.Size {
					request.Pagination.Size = remain
				}
				data = data[offset : offset+request.Pagination.Size]
			}
		}
		for i := range data {
			row := &data[i]
			if row.TransactionInfo != nil {
				err = yson.Unmarshal([]byte(row.TransactionInfo.(string)), &row.TransactionInfo)
				if err != nil {
					return
				}
			}
		}
		result = AccessLogResponse{
			Accesses:      data,
			TotalRowCount: totalRowCount,
		}
		return
	}
}

func ReadAccessLogRequest(req *http.Request) (request AccessLogRequest, err error) {
	decoder := json.NewDecoder(req.Body)
	err = decoder.Decode(&request)
	return
}

func GetClickHouseQueryHandler(snapshotRoot string, authorizer *Authorizer) ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		request, err := ReadAccessLogRequest(req)
		if err != nil {
			return
		}
		user, _, err := authorizer.getUser(req, request.Cluster)
		if err != nil {
			return
		}
		if _, ok := ClustersVisibleRanges[request.Cluster]; !ok {
			err = fmt.Errorf("cluster '%s' not served", request.Cluster)
			return
		}
		query, settings, err := GetQuery(user.Login, request, snapshotRoot, request.Pagination.Index, request.Pagination.Size)
		if err != nil {
			return
		}
		result = ClickHouseQueryResponse{
			Query:    query,
			Settings: settings,
		}
		return
	}
}

func GetQueryTrackerHandler(chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable string, authorizer *Authorizer) ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		request, err := ReadAccessLogRequest(req)
		if err != nil {
			return
		}
		user, _, err := authorizer.getUser(req, request.Cluster)
		if err != nil {
			return
		}
		if _, ok := ClustersVisibleRanges[request.Cluster]; !ok {
			err = fmt.Errorf("cluster '%s' not served", request.Cluster)
			return
		}
		query, settings, err := GetQuery(user.Login, request, snapshotRoot, request.Pagination.Index, request.Pagination.Size)
		if err != nil {
			return
		}
		settings["cluster"] = chytCluster
		settings["clique"] = chytAlias

		reqTimeout := time.Duration(5 * time.Second)
		ytConfig := yt.Config{
			Proxy:               chytCluster,
			Logger:              ytmsvc.Logger,
			LightRequestTimeout: &reqTimeout,
			Token:               ytmsvc.TokenFromEnvVariable(ytTokenEnvVariable),
		}
		ytClient, err := ythttp.NewClient(&ytConfig)
		if err != nil {
			return
		}
		options := yt.StartQueryOptions{
			Settings:             settings,
			AccessControlObjects: &[]string{"everyone-share"},
		}
		queryID, err := ytClient.StartQuery(req.Context(), yt.QueryEngineCHYT, query, &options)
		if err != nil {
			return
		}
		return QTAccessLogResponse{
			Cluster: chytCluster,
			QueryID: queryID.String(),
		}, nil
	}
}

func GetVisibleTimeRange(chytCluster, chytAlias, snapshotRoot, cluster, ytTokenEnvVariable string) (result VisibleTimeRangeResponse, err error) {
	query, settings := GetVisibleTimeRangeQuery(snapshotRoot, cluster)
	settings["priority"] = "0"
	response, err := DoClickhouseQuery[VisibleTimeRangeResponse](chytCluster, chytAlias, query, settings, ytTokenEnvVariable)
	if err != nil {
		return
	}
	result = response[0]
	if result.Earliest == 0 && result.Latests == 0 {
		err = fmt.Errorf("BUG: VisibleTimeRange is empty")
	}
	return
}

func GetVisibleTimeRangeHandler(chytCluster, chytAlias, snapshotRoot string) ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		decoder := json.NewDecoder(req.Body)
		var request VisibleTimeRangeRequest
		err = decoder.Decode(&request)
		if err != nil {
			return
		}
		if visibleRange, ok := ClustersVisibleRanges[request.Cluster]; !ok {
			err = fmt.Errorf("cluster '%s' not served", request.Cluster)
			return
		} else {
			return visibleRange, nil
		}
	}
}

func GetRouterHandler(chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable string, authorizer *Authorizer, corsConfig *ytmsvc.CORSConfig) http.Handler {
	apiV1 := func(r chi.Router) {
		r.Get("/ready", func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprintf(w, "true") })
		r.Post("/info", ytmsvc.FormatResponse(GetInfoHandler()))
		r.Post("/whoami", ytmsvc.FormatResponse(GetWhoamiHandler(authorizer)))
		r.Post("/{call:served(-|_)clusters}", ytmsvc.FormatResponse(GetServedClustersHandler()))
		r.Post("/{call:access(-|_)log}", ytmsvc.FormatResponse(GetAccessLogHandler(chytCluster, chytAlias, snapshotRoot, authorizer, ytTokenEnvVariable)))
		r.Post("/chquery", ytmsvc.FormatResponse(GetClickHouseQueryHandler(snapshotRoot, authorizer)))
		r.Post("/{call:qt(-|_)access(-|_)log}", ytmsvc.FormatResponse(GetQueryTrackerHandler(chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable, authorizer)))
		r.Post("/{call:visible(-|_)time(-|_)range}", ytmsvc.FormatResponse(GetVisibleTimeRangeHandler(chytCluster, chytAlias, snapshotRoot)))
	}
	r := chi.NewRouter()
	r.Use(corsHandler(corsConfig))
	r.Use(GetRequestLoggerMiddleware)
	r.Get("/liveness", ytmsvc.FormatResponse(GetLivenessHandler()))
	r.Get("/readiness", ytmsvc.FormatResponse(GetReadinessHandler()))
	r.Get("/metrics", GetMetricsHandler())
	r.Route("/{api_version}", apiV1)
	apiV1(r)
	return r
}

func UpdateClusters(ctx context.Context, chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable string, ytClient yt.Client, cmd *cobra.Command) {
	includeArr := ytmsvc.Must(cmd.Flags().GetStringSlice("include"))
	excludeArr := ytmsvc.Must(cmd.Flags().GetStringSlice("exclude"))
	useClusterDirectory := ytmsvc.Must(cmd.Flags().GetString("proxy")) != ""
	timer := time.NewTimer(0)
	delay := time.Duration(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if clusters := ytmsvc.GetClusters(ctx, ytClient, includeArr, excludeArr, useClusterDirectory); clusters != nil {
				newClustersVisibleRanges := make(map[string]VisibleTimeRangeResponse)
				for cluster := range clusters {
					response, err := GetVisibleTimeRange(chytCluster, chytAlias, snapshotRoot, cluster, ytTokenEnvVariable)
					if err != nil {
						ytmsvc.Logger.Error(err.Error())
						continue
					}
					newClustersVisibleRanges[cluster] = response
				}
				ClustersVisibleRanges = newClustersVisibleRanges
			}
			timer.Reset(delay)
		}
	}
}

func RunServer(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("normal terminate"))
	ytTokenEnvVariable := ytmsvc.Must(cmd.Flags().GetString("token-env-variable"))
	ytClient := ytmsvc.MustNewYTClient(ytmsvc.Must(cmd.Flags().GetString("proxy")), ytTokenEnvVariable)
	chytCluster := ytmsvc.Must(cmd.Flags().GetString("chyt-cluster-name"))
	chytAlias := ytmsvc.Must(cmd.Flags().GetString("chyt-alias"))
	snapshotRoot := ytmsvc.Must(cmd.Flags().GetString("snapshot-root"))
	go UpdateClusters(ctx, chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable, ytClient, cmd)

	authorizerConfig := newAuthorizerConfigFromCmd(cmd)
	authorizer := newAuthorizer(logger, authorizerConfig)

	port := ytmsvc.Must(cmd.Flags().GetUint16("port"))

	corsConfig := ytmsvc.CORSConfig{
		AllowedHosts:        ytmsvc.Must(cmd.Flags().GetStringSlice("allowed-hosts")),
		AllowedHostSuffixes: ytmsvc.Must(cmd.Flags().GetStringSlice("allowed-host-suffixes")),
	}

	addr := fmt.Sprintf(":%v", port)
	router := GetRouterHandler(chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable, authorizer, &corsConfig)

	ytmsvc.Must0(http.ListenAndServe(addr, router))
}

func main() {
	//logger = ytlog.Must()
	logger = ytmsvc.Must(zap.NewDeployLogger(log.DebugLevel))
	logger.Info("start")
	ytmsvc.Logger = logger
	ParseArgsAndRunServer()
}
