//go:build !internal
// +build !internal

package main

import (
	"net/http"
	"os"

	"github.com/spf13/cobra"

	"go.ytsaurus.tech/library/go/core/metrics/prometheus"
	"go.ytsaurus.tech/yt/go/yt"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

// @State external
// @Produce json
// @Success 200 {object} bac_lib.WhoamiResponse
// @Router /whoami [post]
func GetWhoamiHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		return bac_lib.WhoamiResponse{
			User: "user",
		}, nil
	}
}

type externalAccessChecker struct{}

func (c *externalAccessChecker) CheckAccess(subject string, req *http.Request) (string, error) {
	return "user", nil
}

func GetCheckACLHandler() ytmsvc.HTTPHandlerE {
	checker := &externalAccessChecker{}
	return createCheckACLHandler(checker)
}

func GetRouterHandler(_ yt.Client, _ *cobra.Command) http.Handler {
	return createRouter(
		GetInfoHandler(),
		GetWhoamiHandler(),
		GetServedClustersHandler(),
		GetCheckACLHandler(),
		GetClickHouseDictHandler(),
		GetLivenessHandler(),
		GetReadinessHandler(),
		GetDropCacheHandler(),
		GetMetricsHandler(),
	)
}

func getHostNameForMetrics() string {
	return ytmsvc.Must(os.Hostname())
}

func getNewRegistry(tags map[string]string) *prometheus.Registry {
	regOpts := prometheus.NewRegistryOpts().SetTags(tags)
	return prometheus.NewRegistry(regOpts)
}

func streamRegistry(reg *prometheus.Registry, w http.ResponseWriter, req *http.Request) {
	_, _ = reg.Stream(req.Context(), w)
}
