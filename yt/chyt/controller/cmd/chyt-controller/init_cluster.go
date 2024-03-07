package main

import (
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"

	"go.ytsaurus.tech/yt/chyt/controller/internal/app"
	"go.ytsaurus.tech/yt/chyt/controller/internal/chyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/jupyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
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

	familyToInitializerFactory := map[string]strawberry.ClusterInitializerFactory{
		"chyt": chyt.NewClusterInitializer,
	}
	if slices.Contains(config.Families, "jupyt") {
		familyToInitializerFactory["jupyt"] = jupyt.NewClusterInitializer
	}

	clusterInitializer := app.NewClusterInitializer(&config, familyToInitializerFactory)
	return clusterInitializer.InitCluster()
}
