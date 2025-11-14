//go:build !internal
// +build !internal

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"

	"go.ytsaurus.tech/library/go/core/metrics/prometheus"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func GetWhoamiHandler() ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (result any, err error) {
		type Response struct {
			User string `json:"user"`
		}
		return Response{
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

func GetRouterHandler(ytClient yt.Client) http.Handler {
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

func RunServer(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("normal terminate"))
	ytClient := ytmsvc.MustNewYTClient(ytmsvc.Must(cmd.Flags().GetString("proxy")))
	go perClusterRunner(ctx, ytClient, cmd)
	port := ytmsvc.Must(cmd.Flags().GetUint16("port"))

	addr := fmt.Sprintf(":%v", port)
	router := GetRouterHandler(ytClient)
	ytmsvc.Must0(http.ListenAndServe(addr, router))
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
