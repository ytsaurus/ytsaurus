//go:build !internal
// +build !internal

package main

import (
	"github.com/spf13/cobra"
)

func extendRootCmd(rootCmd *cobra.Command) {
	rootCmd.Long = "https://github.com/ytsaurus/ytsaurus/tree/main/yt/microservices/access_log_viewer/README.md"

	rootCmd.Flags().StringSlice("allowed-hosts", nil, "Allowed hosts for CORS")
	rootCmd.Flags().StringSlice("allowed-host-suffixes", nil, "Allowed hosts suffixes for CORS")

	rootCmd.Flags().String("auth-cookie-name", "YTCypressCookie", "The name of the cookie by which the service will attempt to authenticate the request.")

	rootCmd.Flags().String("proxy", "", "The cluster proxy from which to obtain information about the clusters that will be included in the `include` parameter. Clusters are specified as keys of the `//sys/clusters` node of the `document` type.")
	rootCmd.Flags().String("chyt-cluster-name", "", "CHYT cluster to operate")
}
