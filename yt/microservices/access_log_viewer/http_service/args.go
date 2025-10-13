package main

import (
	"github.com/spf13/cobra"

	lib "go.ytsaurus.tech/yt/microservices/lib/go"
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

	lib.Must0(rootCmd.Execute())
}
