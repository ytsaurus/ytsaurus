//go:build !internal
// +build !internal

package main

import (
	"net/http"

	"github.com/spf13/cobra"

	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func corsHandler(conf *ytmsvc.CORSConfig) func(next http.Handler) http.Handler {
	return ytmsvc.CORS(conf)
}

func newCORSConfigFromCmd(cmd *cobra.Command) *ytmsvc.CORSConfig {
	return &ytmsvc.CORSConfig{
		AllowedHosts:        ytmsvc.Must(cmd.Flags().GetStringSlice("allowed-hosts")),
		AllowedHostSuffixes: ytmsvc.Must(cmd.Flags().GetStringSlice("allowed-host-suffixes")),
	}
}
