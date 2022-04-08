package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/ytprof/internal/fetcher"
)

var (
	flagConfig string
	flagProxy  string

	YT yt.Client
)

var rootCmd = &cobra.Command{
	Use: "ytprof",

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		YT, err = ythttp.NewClient(&yt.Config{
			Proxy:             flagProxy,
			ReadTokenFromFile: true,
		})
		return err
	},
}

var runFetcherCmd = &cobra.Command{
	Use:   "run-fetcher",
	Short: "Manualy run profile fetcher based on config",
	RunE:  runFetcher,
	Args:  cobra.ExactArgs(0),
}

func init() {
	rootCmd.PersistentFlags().StringVar(&flagProxy, "proxy", "", "name of the YT cluster, e.g \"hume\"")
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

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

func main() {
	Execute()
}
