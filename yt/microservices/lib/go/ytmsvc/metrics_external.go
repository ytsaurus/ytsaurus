//go:build !internal
// +build !internal

package ytmsvc

import (
	"net/http"
	"os"

	"go.ytsaurus.tech/library/go/core/metrics/prometheus"
)

func GetHostNameForMetrics() string {
	return Must(os.Hostname())
}

func GetNewRegistry(tags map[string]string) *prometheus.Registry {
	regOpts := prometheus.NewRegistryOpts().SetTags(tags)
	return prometheus.NewRegistry(regOpts)
}

func StreamRegistry(reg *prometheus.Registry, w http.ResponseWriter, req *http.Request) {
	_, _ = reg.Stream(req.Context(), w)
}
