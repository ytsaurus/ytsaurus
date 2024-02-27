package main

import (
	"github.com/spf13/cobra"

	"go.ytsaurus.tech/yt/chyt/controller/internal/app"
	"go.ytsaurus.tech/yt/chyt/controller/internal/chyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/jupyt"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
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

	cfs := map[string]strawberry.ControllerFactory{}

	// CHYT controller is always present.

	chytConfig, ok := config.Controllers["chyt"]
	if !ok {
		chytConfig = config.Controller
	}
	chytFactory := strawberry.ControllerFactory{
		Ctor:   chyt.NewController,
		Config: chytConfig,
	}
	cfs["chyt"] = chytFactory

	// JUPYT controller is optional.

	if jupytConfig, ok := config.Controllers["jupyt"]; ok {
		jupytFactory := strawberry.ControllerFactory{
			Ctor:          jupyt.NewController,
			Config:        jupytConfig,
			ExtraCommands: jupyt.AllCommands,
		}
		cfs["jupyt"] = jupytFactory
	}

	a := app.New(&config, &options, cfs)
	a.Run(make(chan struct{}) /*stopCh*/)
	panic("unreachable")
}
