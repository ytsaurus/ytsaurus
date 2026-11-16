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
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/ypath"
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

type clusterState struct {
	VisibleRange *VisibleTimeRangeResponse
	Export       ExportClusterStatus
}

func (cs clusterState) status() ClusterStatus {
	status := ClusterStatus{Export: cs.Export}
	if cs.VisibleRange != nil {
		status.AccessLog.Available = true
	} else {
		status.AccessLog.Reason = "chyt_unavailable"
	}
	return status
}

// TODO(ilyaibraev): Synchronize cluster-state publication and reads.
var clusterStates map[string]clusterState

func GetLivenessHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		return ytmsvc.ResponseStatusOK, nil
	}
}

func GetReadinessHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		for _, state := range clusterStates {
			if state.VisibleRange != nil || state.Export.InputTablesAvailable {
				return ytmsvc.ResponseStatusOK, nil
			}
		}
		return nil, yterrors.Err("not ready")
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
		response := ServedClustersResponse{Clusters: []string{}, Statuses: make(map[string]ClusterStatus)}
		for cluster, state := range clusterStates {
			status := state.status()
			if status.AccessLog.Available {
				response.Clusters = append(response.Clusters, cluster)
			}
			response.Statuses[cluster] = status
		}
		return response, nil
	}
}

func GetMetricsHandler() http.HandlerFunc {
	host := ytmsvc.GetHostNameForMetrics()
	return func(w http.ResponseWriter, req *http.Request) {
		now := time.Now().Unix()
		reg := ytmsvc.GetNewRegistry(map[string]string{"host": host})
		for cluster, state := range clusterStates {
			if state.VisibleRange != nil {
				subreg := reg.WithTags(map[string]string{"served_cluster": cluster})
				subreg.IntGauge("data_lag").Set(now - state.VisibleRange.Latests)
			}
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

func buildChytUrl(chytClusterProxy, chytAlias, paramsStr string) (string, error) {
	ytConfig := yt.Config{
		Proxy: chytClusterProxy,
	}
	clusterUrl, err := ytConfig.GetClusterURL()
	if err != nil {
		return "", fmt.Errorf("cannot resolve CHYT cluster URL: %w", err)
	}

	return fmt.Sprintf("%s://%s/query?database=*%s&%s",
		clusterUrl.Scheme, clusterUrl.Address, chytAlias, paramsStr), nil
}

func DoClickhouseQuery[T any](chytCluster, chytAlias, query string, settings map[string]string, ytTokenEnvVariable string) ([]T, error) {
	var params []string
	for k, v := range settings {
		param := fmt.Sprintf("%s=%s", k, url.QueryEscape(v))
		params = append(params, param)
	}
	params = append(params, "default_format=JSONEachRow")
	params = append(params, "output_format_json_quote_64bit_integers=0")
	paramsStr := strings.Join(params, "&")

	url, err := buildChytUrl(chytCluster, chytAlias, paramsStr)
	if err != nil {
		return nil, err
	}
	body, err := DoClickhouseHTTPRequestWithRedirectAndPreserveAuthorization(url, []byte(query), ytTokenEnvVariable)
	if err != nil {
		return nil, err
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
				if strings.Contains(err.Error(), "No tables to read from") || strings.Contains(err.Error(), "Both table name and UUID are empty") {
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

func GetAccessLogHandler(chytCluster, chytAlias, snapshotRoot string, authorizer *Authorizer, ytTokenEnvVariable, logsTimezone string) ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		request, err := ReadAccessLogRequest(req)
		if err != nil {
			return
		}
		user, _, err := authorizer.getUser(req, request.Cluster)
		if err != nil {
			return
		}
		if clusterStates[request.Cluster].VisibleRange == nil {
			err = fmt.Errorf("cluster '%s' not served", request.Cluster)
			return
		}
		query, settings, err := GetQuery(user.Login, request, snapshotRoot, 0, 0, logsTimezone)
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

func GetClickHouseQueryHandler(snapshotRoot, logsTimezone string, authorizer *Authorizer) ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		request, err := ReadAccessLogRequest(req)
		if err != nil {
			return
		}
		user, _, err := authorizer.getUser(req, request.Cluster)
		if err != nil {
			return
		}
		if clusterStates[request.Cluster].VisibleRange == nil {
			err = fmt.Errorf("cluster '%s' not served", request.Cluster)
			return
		}
		query, settings, err := GetQuery(user.Login, request, snapshotRoot, request.Pagination.Index, request.Pagination.Size, logsTimezone)
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

func GetQueryTrackerHandler(chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable, logsTimezone string, authorizer *Authorizer) ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		request, err := ReadAccessLogRequest(req)
		if err != nil {
			return
		}
		user, _, err := authorizer.getUser(req, request.Cluster)
		if err != nil {
			return
		}
		if clusterStates[request.Cluster].VisibleRange == nil {
			err = fmt.Errorf("cluster '%s' not served", request.Cluster)
			return
		}
		query, settings, err := GetQuery(user.Login, request, snapshotRoot, request.Pagination.Index, request.Pagination.Size, logsTimezone)
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

func GetVisibleTimeRange(chytCluster, chytAlias, snapshotRoot, cluster, ytTokenEnvVariable, logsTimezone string) (result VisibleTimeRangeResponse, err error) {
	query, settings := GetVisibleTimeRangeQuery(snapshotRoot, cluster, logsTimezone)
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
		if visibleRange := clusterStates[request.Cluster].VisibleRange; visibleRange == nil {
			err = fmt.Errorf("cluster '%s' not served", request.Cluster)
			return
		} else {
			return *visibleRange, nil
		}
	}
}

func GetRouterHandler(chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable, logsTimezone string, authorizer *Authorizer, exporter *AccessLogExporter, corsConfig *ytmsvc.CORSConfig) http.Handler {
	apiV1 := func(r chi.Router) {
		if exporter != nil {
			exporter.routes(r)
		}
		r.Get("/ready", func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprintf(w, "true") })
		r.Post("/info", ytmsvc.FormatResponse(GetInfoHandler()))
		r.Post("/whoami", ytmsvc.FormatResponse(GetWhoamiHandler(authorizer)))
		r.Post("/{call:served(-|_)clusters}", ytmsvc.FormatResponse(GetServedClustersHandler()))
		r.Post("/{call:access(-|_)log}", ytmsvc.FormatResponse(GetAccessLogHandler(chytCluster, chytAlias, snapshotRoot, authorizer, ytTokenEnvVariable, logsTimezone)))
		r.Post("/chquery", ytmsvc.FormatResponse(GetClickHouseQueryHandler(snapshotRoot, logsTimezone, authorizer)))
		r.Post("/{call:qt(-|_)access(-|_)log}", ytmsvc.FormatResponse(GetQueryTrackerHandler(chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable, logsTimezone, authorizer)))
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

func UpdateClusters(ctx context.Context, chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable, logsTimezone string, ytClient yt.Client, exporter *AccessLogExporter, cmd *cobra.Command) {
	includeArr := ytmsvc.Must(cmd.Flags().GetStringSlice("include"))
	excludeArr := ytmsvc.Must(cmd.Flags().GetStringSlice("exclude"))
	useClusterDirectory := ytmsvc.Must(cmd.Flags().GetString("proxy")) != ""
	timer := time.NewTimer(0)
	defer timer.Stop()
	delay := time.Duration(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if clusters := ytmsvc.GetClusters(ctx, ytClient, includeArr, excludeArr, useClusterDirectory); clusters != nil {
				states := make(map[string]clusterState)
				for cluster := range clusters {
					state := clusterState{Export: ExportClusterStatus{Reason: "disabled"}}
					response, err := GetVisibleTimeRange(chytCluster, chytAlias, snapshotRoot, cluster, ytTokenEnvVariable, logsTimezone)
					if err != nil {
						ytmsvc.Logger.Error(err.Error())
					} else {
						state.VisibleRange = &response
					}
					if exporter != nil {
						state.Export = exporter.getClusterStatus(ctx, cluster)
					}
					states[cluster] = state
				}
				clusterStates = states
			}
			timer.Reset(delay)
		}
	}
}

const exportClientTimeout = 30 * time.Second

func RunServer(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("normal terminate"))
	ytTokenEnvVariable := ytmsvc.Must(cmd.Flags().GetString("token-env-variable"))
	ytClient := ytmsvc.MustNewYTClient(ytmsvc.Must(cmd.Flags().GetString("proxy")), ytTokenEnvVariable)
	chytCluster := ytmsvc.Must(cmd.Flags().GetString("chyt-cluster-name"))
	chytAlias := ytmsvc.Must(cmd.Flags().GetString("chyt-alias"))
	snapshotRoot := ytmsvc.Must(cmd.Flags().GetString("snapshot-root"))
	logsTimezone := ytmsvc.Must(cmd.Flags().GetString("logs-timezone"))
	authorizerConfig := newAuthorizerConfigFromCmd(cmd)
	authorizer := newAuthorizer(logger, authorizerConfig)

	exportOperationCluster := ytmsvc.Must(cmd.Flags().GetString("export-operation-cluster"))
	var exporter *AccessLogExporter
	if exportOperationCluster != "" {
		operationClusterClient := ytmsvc.Must(newYTClient(exportOperationCluster, ytTokenEnvVariable, logger, exportClientTimeout))
		exporter = ytmsvc.Must(NewAccessLogExporter(AccessLogExporterConfig{
			OperationCluster:   exportOperationCluster,
			SnapshotRoot:       ypath.Path(snapshotRoot),
			LogsTimezone:       logsTimezone,
			TokenEnvVariable:   ytTokenEnvVariable,
			DefaultPool:        ytmsvc.Must(cmd.Flags().GetString("export-default-pool")),
			PoolTree:           ytmsvc.Must(cmd.Flags().GetString("export-pool-tree")),
			MaxInputTableCount: ytmsvc.Must(cmd.Flags().GetInt("export-max-input-tables")),
		}, operationClusterClient, authorizer, logger))
	}
	go UpdateClusters(ctx, chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable, logsTimezone, ytClient, exporter, cmd)

	port := ytmsvc.Must(cmd.Flags().GetUint16("port"))

	corsConfig := newCORSConfigFromCmd(cmd)

	host := ""
	if authorizerConfig.debugLogin != "" {
		host = "127.0.0.1"
	}
	addr := fmt.Sprintf("%s:%v", host, port)
	router := GetRouterHandler(chytCluster, chytAlias, snapshotRoot, ytTokenEnvVariable, logsTimezone, authorizer, exporter, corsConfig)

	ytmsvc.Must0(http.ListenAndServe(addr, router))
}

func main() {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}
	//logger = ytlog.Must()
	logger = ytmsvc.Must(zap.NewDeployLogger(log.DebugLevel))
	logger.Info("start")
	ytmsvc.Logger = logger
	ParseArgsAndRunServer()
}
