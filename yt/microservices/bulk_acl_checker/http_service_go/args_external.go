//go:build !internal
// +build !internal

package main

import (
	"github.com/spf13/cobra"
)

func extendRootCmd(rootCmd *cobra.Command) {
	rootCmd.Long = "https://github.com/ytsaurus/ytsaurus/tree/main/yt/microservices/bulk_acl_checker/README.md"
	rootCmd.Flags().String("proxy", "", "The cluster proxy from which to obtain information about the clusters that will be included in the `include` parameter. Clusters are specified as keys of the `//sys/clusters` node of the `document` type.")
}
