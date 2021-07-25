package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"

	"a.yandex-team.ru/yt/chyt/controller/internal/app"
	"a.yandex-team.ru/yt/chyt/controller/internal/chyt"
	"a.yandex-team.ru/yt/go/yson"
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

	content, err := ioutil.ReadFile(flagConfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading config file: %v\n", err)
		os.Exit(1)
	}
	err = yson.Unmarshal(content, &config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing yson config: %v\n", err)
		os.Exit(1)
	}

	options := app.Options{
		LogToStderr: flagLogToStderr,
	}

	a := app.New(&config, &options, chyt.NewController)
	a.Run()
	panic("unreachable")
}
