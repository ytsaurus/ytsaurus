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
	lib "go.ytsaurus.tech/yt/microservices/lib/go"
)

const QueryTrackerRowCountLimitStr = "10000" // https://go.ytsaurus.tech/arcadia/yt/yt/server/query_tracker/config.cpp?rev=r14742253#L21

var logger *zap.Logger

var ClustersVisibleRanges = make(map[string]VisibleTimeRangeResponse)

func GetLivenessHandler() lib.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		return lib.ResponseStatusOK, nil
	}
}

func GetReadinessHandler() lib.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		if len(ClustersVisibleRanges) == 0 {
			return nil, yterrors.Err("not ready")
		}
		return lib.ResponseStatusOK, nil
	}
}

func GetInfoHandler() lib.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		return
	}
}

func GetWhoamiHandler(authorizer *Authorizer) lib.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		type Response struct {
			User string `json:"user"`
		}
		user, ticket, err := authorizer.getUser(req)
		_ = ticket
		if err != nil {
			return
		} else {
			return Response{
				User: user.Login,
			}, nil
		}
	}
}

func GetServedClustersHandler() lib.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		response := ServedClustersResponse{}
		for cluster := range ClustersVisibleRanges {
			response.Clusters = append(response.Clusters, cluster)
		}
		return response, nil
	}
}

func GetMetricsHandler() http.HandlerFunc {
	host := lib.GetHostNameForMetrics()
	return func(w http.ResponseWriter, req *http.Request) {
		now := time.Now().Unix()
		reg := lib.GetNewRegistry(map[string]string{"host": host})
		for cluster, visibleRange := range ClustersVisibleRanges {
			subreg := reg.WithTags(map[string]string{"served_cluster": cluster})
			subreg.IntGauge("data_lag").Set(now - visibleRange.Latests)
		}
		lib.StreamRegistry(reg, w, req)
	}
}

func GetRequestLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := lib.GetRequestSomeID(r)
		logger.Debugf("Start processing query: %s %s", id, r.URL)
		next.ServeHTTP(w, r)
		logger.Debugf("Finish processing query: %s %s", id, r.URL)
	})
}

func DoClickhouseHTTPRequestWithRedirectAndPreserveAuthorization(urlStr string, data []byte) (result io.ReadCloser, err error) {
	httpReq, err := http.NewRequest("POST", urlStr, bytes.NewBuffer(data))
	if err != nil {
		return
	}
	httpReq.Header.Set("Authorization", "OAuth "+os.Getenv("YT_TOKEN"))
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
		return DoClickhouseHTTPRequestWithRedirectAndPreserveAuthorization(location.String(), data)
	}

	result = httpResp.Body
	return
}

func DoClickhouseQuery[T any](chytCluster, chytAlias, query string, settings map[string]string) (result []T, err error) {
	var params []string
	for k, v := range settings {
		param := fmt.Sprintf("%s=%s", k, url.QueryEscape(v))
		params = append(params, param)
	}
	params = append(params, "default_format=JSONEachRow")
	params = append(params, "output_format_json_quote_64bit_integers=0")
	paramsStr := strings.Join(params, "&")

	url := fmt.Sprintf("http://%s.yt.yandex.net/query?database=*%s&%s", chytCluster, chytAlias, paramsStr)
	body, err := DoClickhouseHTTPRequestWithRedirectAndPreserveAuthorization(url, []byte(query))
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

func GetAccessLogHandler(chytCluster, chytAlias, snapshotRoot string, authorizer *Authorizer) lib.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		user, _, err := authorizer.getUser(req)
		if err != nil {
			return
		}
		request, err := ReadAccessLogRequest(req)
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
		data, err := DoClickhouseQuery[AccessRecord](chytCluster, chytAlias, query, settings)
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

func GetClickHouseQueryHandler(snapshotRoot string, authorizer *Authorizer) lib.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		user, _, err := authorizer.getUser(req)
		if err != nil {
			return
		}
		request, err := ReadAccessLogRequest(req)
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

func GetQueryTrackerHandler(chytCluster, chytAlias, snapshotRoot string, authorizer *Authorizer) lib.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		user, _, err := authorizer.getUser(req)
		if err != nil {
			return
		}
		request, err := ReadAccessLogRequest(req)
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
			Logger:              lib.Logger,
			LightRequestTimeout: &reqTimeout,
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

func GetVisibleTimeRange(chytCluster, chytAlias, snapshotRoot, cluster string) (result VisibleTimeRangeResponse, err error) {
	query, settings := GetVisibleTimeRangeQuery(snapshotRoot, cluster)
	settings["priority"] = "0"
	response, err := DoClickhouseQuery[VisibleTimeRangeResponse](chytCluster, chytAlias, query, settings)
	if err != nil {
		return
	}
	result = response[0]
	if result.Earliest == 0 && result.Latests == 0 {
		err = fmt.Errorf("BUG: VisibleTimeRange is empty")
	}
	return
}

func GetVisibleTimeRangeHandler(chytCluster, chytAlias, snapshotRoot string) lib.HTTPHandlerE {
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

func GetRouterHandler(chytCluster, chytAlias, snapshotRoot string, authorizer *Authorizer, corsConfig *lib.CORSConfig) http.Handler {
	apiV1 := func(r chi.Router) {
		r.Get("/ready", func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprintf(w, "true") })
		r.Post("/info", lib.FormatResponse(GetInfoHandler()))
		r.Post("/whoami", lib.FormatResponse(GetWhoamiHandler(authorizer)))
		r.Post("/{call:served(-|_)clusters}", lib.FormatResponse(GetServedClustersHandler()))
		r.Post("/{call:access(-|_)log}", lib.FormatResponse(GetAccessLogHandler(chytCluster, chytAlias, snapshotRoot, authorizer)))
		r.Post("/chquery", lib.FormatResponse(GetClickHouseQueryHandler(snapshotRoot, authorizer)))
		r.Post("/{call:qt(-|_)access(-|_)log}", lib.FormatResponse(GetQueryTrackerHandler(chytCluster, chytAlias, snapshotRoot, authorizer)))
		r.Post("/{call:visible(-|_)time(-|_)range}", lib.FormatResponse(GetVisibleTimeRangeHandler(chytCluster, chytAlias, snapshotRoot)))
	}
	r := chi.NewRouter()
	r.Use(corsHandler(corsConfig))
	r.Use(GetRequestLoggerMiddleware)
	r.Get("/liveness", lib.FormatResponse(GetLivenessHandler()))
	r.Get("/readiness", lib.FormatResponse(GetReadinessHandler()))
	r.Get("/metrics", GetMetricsHandler())
	r.Route("/{api_version}", apiV1)
	apiV1(r)
	return r
}

func UpdateClusters(ctx context.Context, chytCluster, chytAlias, snapshotRoot string, ytClient yt.Client, cmd *cobra.Command) {
	includeArr := lib.Must(cmd.Flags().GetStringSlice("include"))
	excludeArr := lib.Must(cmd.Flags().GetStringSlice("exclude"))
	useClusterDirectory := lib.Must(cmd.Flags().GetString("proxy")) != ""
	timer := time.NewTimer(0)
	delay := time.Duration(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if clusters := lib.GetClusters(ctx, ytClient, includeArr, excludeArr, useClusterDirectory); clusters != nil {
				newClustersVisibleRanges := make(map[string]VisibleTimeRangeResponse)
				for cluster := range clusters {
					response, err := GetVisibleTimeRange(chytCluster, chytAlias, snapshotRoot, cluster)
					if err != nil {
						lib.Logger.Error(err.Error())
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
	ytClient := lib.MustNewYTClient(lib.Must(cmd.Flags().GetString("proxy")))
	chytCluster := lib.Must(cmd.Flags().GetString("chyt-cluster"))
	chytAlias := lib.Must(cmd.Flags().GetString("chyt-alias"))
	snapshotRoot := lib.Must(cmd.Flags().GetString("snapshot-root"))
	go UpdateClusters(ctx, chytCluster, chytAlias, snapshotRoot, ytClient, cmd)

	authorizerConfig := newAuthorizerConfigFromCmd(cmd)
	authorizer := newAuthorizer(logger, authorizerConfig)

	port := lib.Must(cmd.Flags().GetUint16("port"))

	corsConfig := lib.CORSConfig{
		AllowedHosts:        lib.Must(cmd.Flags().GetStringSlice("allowed-hosts")),
		AllowedHostSuffixes: lib.Must(cmd.Flags().GetStringSlice("allowed-hosts-suffixes")),
	}

	addr := fmt.Sprintf(":%v", port)
	router := GetRouterHandler(chytCluster, chytAlias, snapshotRoot, authorizer, &corsConfig)

	lib.Must0(http.ListenAndServe(addr, router))
}

func main() {
	//logger = ytlog.Must()
	logger = lib.Must(zap.NewDeployLogger(log.DebugLevel))
	logger.Info("start")
	lib.Logger = logger
	ParseArgsAndRunServer()
}
