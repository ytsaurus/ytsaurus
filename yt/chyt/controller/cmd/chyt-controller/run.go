package main

import (
	"github.com/spf13/cobra"
	"go.ytsaurus.tech/yt/chyt/controller/internal/app"
	"go.ytsaurus.tech/yt/chyt/controller/internal/chyt"
)

var runCmd = &cobra.Command{
	Use: "run",
	Run: wrapRun(doRun),
}

var (
	flagForceFlush bool
)

func init() {
	runCmd.PersistentFlags().BoolVar(&flagForceFlush, "force-flush", false,
		"if set, all operation states will be forcefully flushed after attaching")
	rootCmd.AddCommand(runCmd)
}

func doRun() error {
	var config app.Config
	loadConfig(flagConfigPath, &config)
	options := app.Options{
		LogToStderr: flagLogToStderr,
	}
	a := app.New(&config, &options, chyt.NewController)
	a.Run()
	panic("unreachable")
}
