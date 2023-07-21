package main

import (
	"github.com/spf13/cobra"
	"go.ytsaurus.tech/yt/chyt/controller/internal/app"
	"go.ytsaurus.tech/yt/chyt/controller/internal/chyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
)

var oneShotRunCmd = &cobra.Command{
	Use: "one-shot-run",
	Run: wrapRun(doOneShotRun),
}

var (
	flagCliqueAlias string
	flagSpecletPath string
)

func init() {
	oneShotRunCmd.PersistentFlags().StringVar(&flagCliqueAlias, "alias", "", "clique alias")
	if err := oneShotRunCmd.MarkPersistentFlagRequired("alias"); err != nil {
		panic(err)
	}
	oneShotRunCmd.PersistentFlags().StringVar(&flagSpecletPath, "speclet-path", "", "path to the clique speclet")
	if err := oneShotRunCmd.MarkPersistentFlagRequired("speclet-path"); err != nil {
		panic(err)
	}
	rootCmd.AddCommand(oneShotRunCmd)
}

func doOneShotRun() error {
	var config app.OneShotRunnerConfig
	loadConfig(flagConfigPath, &config)
	config.StrawberryRoot = getStrawberryRoot(config.StrawberryRoot)
	options := app.Options{
		LogToStderr: flagLogToStderr,
	}
	specletYson := readConfig(flagSpecletPath)
	// TODO(max42): extend for generic controllers.
	runner := app.NewOneShotRunner(&config, &options, strawberry.ControllerFactory{Factory: chyt.NewController, Config: config.Controller})
	return runner.Run(flagCliqueAlias, specletYson)
}
