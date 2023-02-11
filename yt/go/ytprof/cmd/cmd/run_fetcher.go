package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"a.yandex-team.ru/library/go/core/log"
	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/internal/fetcher"
	"a.yandex-team.ru/yt/go/ytprof/internal/storage"
)

var (
	flagConfig      string
	flagLogsDir     string
	flagLogToStderr bool
)

var runFetcherCmd = &cobra.Command{
	Use:   "run-fetcher",
	Short: "Manually run profile fetcher based on config",
	RunE:  runFetcher,
	Args:  cobra.ExactArgs(0),
}

func init() {
	runFetcherCmd.Flags().StringVar(&flagConfig, "config", "", "config to run fetcher from")
	runFetcherCmd.Flags().StringVar(&flagLogsDir, "log-dir", "/logs", "path to the log directory")
	runFetcherCmd.Flags().BoolVar(&flagLogToStderr, "log-to-stderr", false, "write logs to stderr")

	rootCmd.AddCommand(runFetcherCmd)
}

func newLogger(name string) *logzap.Logger {
	if flagLogToStderr {
		return ytlog.Must()
	}

	l, _, err := ytlog.NewSelfrotate(filepath.Join(flagLogsDir, name+".log"))
	if err != nil {
		panic(err)
	}

	return l
}

func runFetcher(cmd *cobra.Command, args []string) error {
	l := newLogger("ytprof-fetcher")

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

	data, err := io.ReadAll(file)
	if err != nil {
		l.Fatal("reading file failed", log.Error(err), log.String("config_path", flagConfig))
		return err
	}

	configs := fetcher.Configs{}

	err = yson.Unmarshal(data, &configs)
	if err != nil {
		l.Fatal("unmarshalling file failed", log.Error(err), log.String("config_path", flagConfig))
		return err
	}

	configs.FillInfo(context.Background(), l)

	l.Debug("config reading succeeded", log.String("config", fmt.Sprintf("%v", configs)), log.String("config_path", flagConfig))

	errs, _ := errgroup.WithContext(context.Background())
	ts := storage.NewTableStorage(YT, ypath.Path(flagTablePath), l)
	err = ytprof.MigrateTables(YT, ypath.Path(flagTablePath))
	if err != nil {
		l.Fatal("migration failed", log.Error(err), log.String("table_path", flagTablePath))
		return err
	}

	l.Debug("migration succeeded", log.String("table_path", flagTablePath))

	for _, config := range configs.Configs {
		f := fetcher.NewFetcher(YT, config, l, ts, flagTablePath)
		errs.Go(func() error {
			return f.RunFetcherContinuous()
		})
	}
	return errs.Wait()
}
