package main

import (
	"github.com/spf13/cobra"

	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func ParseArgsAndRunServer() {
	var rootCmd = &cobra.Command{
		Use:   "http_server",
		Short: "Check user access to some path on YT cluster.",
		Run:   RunServer,
	}

	extendRootCmd(rootCmd)

	rootCmd.Flags().Uint16P("port", "p", 80, "HTTP Server port")
	rootCmd.Flags().Uint16P("concurrency", "c", 1, "Load updates concurrency level")
	rootCmd.Flags().StringSliceP("include", "i", nil, "Include clusters to serve")
	rootCmd.Flags().StringSliceP("exclude", "e", nil, "Exclude clusters from serve")
	rootCmd.Flags().String("snapshot-root", "//sys/admin/yt-microservices/bulk_acl_checker", "Path to ACL dumps")
	rootCmd.Flags().String("user-root", "//sys/admin/snapshots/user_exports", "Path to user exports")
	rootCmd.Flags().String("token-env-variable", "YT_BULK_ACL_CHECKER_TOKEN", "Environment variable that specifies the token used when accessing YT")

	ytmsvc.Must0(rootCmd.Execute())
}
