package main

import (
	"github.com/spf13/cobra"

	"go.ytsaurus.tech/yt/chyt/controller/internal/app"
	"go.ytsaurus.tech/yt/chyt/controller/internal/chyt"
)

var initClusterCmd = &cobra.Command{
	Use: "init-cluster",
	Run: wrapRun(doInitCluster),
}

func init() {
	rootCmd.AddCommand(initClusterCmd)
}

func doInitCluster() error {
	var config app.ClusterInitializerConfig
	loadConfig(flagConfigPath, &config)
	config.StrawberryRoot = getStrawberryRoot(config.StrawberryRoot)
	clusterInitializer := app.NewClusterInitializer(&config, chyt.NewClusterInitializer)
	return clusterInitializer.InitCluster()
}
