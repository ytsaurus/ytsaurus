package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/yterrors"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

var logger *zap.Logger

func GetLivenessHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		Cache.Mutex.Lock()
		defer Cache.Mutex.Unlock()
		return ytmsvc.ResponseStatusOK, nil
	}
}

func GetReadinessHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		if !Cache.IsInitialized.Load() {
			return nil, yterrors.Err("not ready")
		}
		return ytmsvc.ResponseStatusOK, nil
	}
}

func GetInfoHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		type ClusterInfo struct {
			Version       string `json:"version"`
			CheckACLLocal bool   `json:"check_acl_local"`
		}
		type Response struct {
			Clusters map[string]ClusterInfo `json:"clusters"`
		}
		response := Response{
			Clusters: make(map[string]ClusterInfo),
		}
		Cache.Mutex.Lock()
		defer Cache.Mutex.Unlock()
		for cluster := range Cache.KnownClusters {
			clusterInfo := ClusterInfo{
				CheckACLLocal: false,
			}
			cacheItem, ok := Cache.Clusters[cluster]
			if ok {
				if cacheItem != nil {
					clusterInfo.CheckACLLocal = len(cacheItem.UsersExport) > 0
					clusterInfo.Version = cacheItem.Version.String()
				} else {
					clusterInfo.Version = "error"
				}
			} else {
				clusterInfo.Version = "not initialized"
			}
			response.Clusters[cluster] = clusterInfo
		}
		return response, nil
	}
}

func GetServedClustersHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		type Response struct {
			Clusters []string `json:"clusters"`
		}
		response := Response{}
		Cache.Mutex.Lock()
		defer Cache.Mutex.Unlock()
		for cluster := range Cache.Clusters {
			response.Clusters = append(response.Clusters, cluster)
		}
		return response, nil
	}
}

func ReadClickhouseDictRequest(input io.Reader) (result []bac_lib.ClickHouseDictRequest, err error) {
	decoder := json.NewDecoder(input)
	for {
		record := bac_lib.ClickHouseDictRequest{}
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
				err = errors.New(string(data))
				return // ClickHouse return error in answer
			}
			return // Invalid ClickHouse answer
		}
		result = append(result, record)
	}
}

func GetDropCacheHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		Cache.LRU.Purge()
		return ytmsvc.ResponseStatusOK, nil
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
func main() {
	//logger = ytlog.Must()
	logger = ytmsvc.Must(zap.NewDeployLogger(log.DebugLevel))
	ytmsvc.Logger = logger
	logger.Info("start")
	ParseArgsAndRunServer()
}
