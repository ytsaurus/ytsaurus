package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/ytprof/internal/fetcher"
)

var (
	flagConfig string
)

var runFetcherCmd = &cobra.Command{
	Use:   "run-fetcher",
	Short: "Manualy run profile fetcher based on config",
	RunE:  runFetcher,
	Args:  cobra.ExactArgs(0),
}

func init() {
	runFetcherCmd.Flags().StringVar(&flagConfig, "config", "", "config to run fetcher from")

	rootCmd.AddCommand(runFetcherCmd)
}

func runFetcher(cmd *cobra.Command, args []string) error {
	l, err := ytlog.New()
	if err != nil {
		panic(err)
	}

	file, err := os.Open(flagConfig)
	if err != nil {
		l.Fatal("opening file failed", log.Error(err), log.String("config_path", flagConfig))
		return err
	}
	defer func() {
		if err = file.Close(); err != nil {
			l.Fatal("closing file failed", log.Error(err), log.String("config_path", flagConfig))
		}
	}()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		l.Fatal("reading file failed", log.Error(err), log.String("config_path", flagConfig))
		return err
	}

	config := fetcher.Config{}

	err = yson.Unmarshal(data, &config)
	if err != nil {
		l.Fatal("unmarshaling file failed", log.Error(err), log.String("config_path", flagConfig))
		return err
	}

	l.Debug("config reading succseded", log.String("config", fmt.Sprintf("%v", config)), log.String("config_path", flagConfig))

	f := fetcher.NewFetcher(YT, config, l)
	return f.RunFetcherContinious()
}
