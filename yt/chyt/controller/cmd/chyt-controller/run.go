package main

import (
	"io/ioutil"

	"github.com/spf13/cobra"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/app"
	"a.yandex-team.ru/yt/chyt/controller/internal/chyt"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
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
	l := newLogger("chyt")

	var config app.Config

	l.Debug("reading config from file", log.String("path", flagConfigPath))
	content, err := ioutil.ReadFile(flagConfigPath)
	if err != nil {
		l.Fatal("error reading config file", log.Error(err))
	}
	err = yson.Unmarshal(content, &config)
	if err != nil {
		l.Fatal("error parsing yson config", log.Error(err))
	}

	options := app.Options{
		ForceFlush: flagForceFlush,
	}

	_ = app.New(l, &config, &options, []strawberry.ControllerFactory{chyt.NewController})
	<-make(chan struct{})

	panic("unreachable")
}
