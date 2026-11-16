package main

import (
	"github.com/spf13/cobra"

	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func ParseArgsAndRunServer() {
	var rootCmd = &cobra.Command{
		Use:   "http_server",
		Short: "Get access log by query",
		Run:   RunServer,
	}

	extendRootCmd(rootCmd)

	rootCmd.Flags().Uint16P("port", "p", 80, "HTTP Server port")
	rootCmd.Flags().Uint16P("concurrency", "c", 1, "Load updates concurrency level")
	rootCmd.Flags().StringSliceP("include", "i", nil, "Include clusters to serve")
	rootCmd.Flags().StringSliceP("exclude", "e", nil, "Exclude clusters from serve")
	rootCmd.Flags().String("chyt-alias", "ch_yt_access_log_viewer", "CHYT alias to operate")
	rootCmd.Flags().String("snapshot-root", "//sys/admin/yt-microservices/access_log_viewer", "Path to ACL dumps")
	rootCmd.Flags().String("token-env-variable", "YT_ACCESS_LOG_VIEWER_TOKEN", "Environment variable that specifies the token used when accessing YT")
	rootCmd.Flags().String("export-operation-cluster", "", "Name of the cluster where access log export operations run and write their results; exports are disabled when empty")
	rootCmd.Flags().String("export-default-pool", "", "Scheduler pool of access log exports that do not specify a pool; when empty, such exports are rejected")
	rootCmd.Flags().String("export-pool-tree", "", "Scheduler pool tree of access log export operations; when empty, use the operation cluster's default tree")
	rootCmd.Flags().Int("export-max-input-tables", 1000, "Maximum number of input tables of an access log export")
	rootCmd.Flags().String("debug-login", "", "Use a static login instead of user authentication and listen on loopback only; intended for local integration tests")

	ytmsvc.Must0(rootCmd.Execute())
}
