//go:build !internal
// +build !internal

package main

import (
	"fmt"
	"net/http"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func corsHandler(conf *ytmsvc.CORSConfig) func(next http.Handler) http.Handler {
	return ytmsvc.CORS(conf)
}

func buildChytUrl(chytClusterProxy, chytAlias, paramsStr string) string {
	clusterUrlStr := chytClusterProxy

	ytConfig := yt.Config{
		Proxy: chytClusterProxy,
	}
	if clusterUrl, err := ytConfig.GetClusterURL(); err == nil {
		clusterUrlStr = clusterUrl.Address
	}

	return fmt.Sprintf("http://%s/query?database=*%s&%s", clusterUrlStr, chytAlias, paramsStr)
}
