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
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/internal/fetcher"
	"a.yandex-team.ru/yt/go/ytprof/internal/storage"
)

var (
	flagConfig      string
	flagPath        string
	flagLogsDir     string
	flagLogToStderr bool
)

var runFetcherCmd = &cobra.Command{
	Use:   "run-fetcher",
	Short: "Manually run profile fetcher based on config",
	RunE:  runFetcher,
	Args:  cobra.ExactArgs(0),
}

var runCypressFetcherCmd = &cobra.Command{
	Use:   "run-cypress-fetcher",
	Short: "Automatic run profile fetcher via per system cypress configs",
	RunE:  runCypressFetcher,
	Args:  cobra.ExactArgs(0),
}

func init() {
	runFetcherCmd.Flags().StringVar(&flagConfig, "config", "", "config to run fetcher from")
	runFetcherCmd.Flags().StringVar(&flagLogsDir, "log-dir", "/logs", "path to the log directory")
	runFetcherCmd.Flags().BoolVar(&flagLogToStderr, "log-to-stderr", false, "write logs to stderr")

	runCypressFetcherCmd.Flags().StringVar(&flagPath, "path", "//home/ytprof", "path to ytprof")
	runCypressFetcherCmd.Flags().StringVar(&flagLogsDir, "log-dir", "/logs", "path to the log directory")
	runCypressFetcherCmd.Flags().BoolVar(&flagLogToStderr, "log-to-stderr", false, "write logs to stderr")

	rootCmd.AddCommand(runFetcherCmd)
	rootCmd.AddCommand(runCypressFetcherCmd)
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

func runCypressFetcher(cmd *cobra.Command, args []string) error {
	l := newLogger("ytprof-fetcher")

	storagePath := ypath.Path(flagPath).Child(ytprof.DirStorage)
	configPath := ypath.Path(flagPath).Child(ytprof.DirConfigs)

	var configDirs []string

	ctx := context.Background()

	err := YT.ListNode(ctx, configPath, &configDirs, nil)
	if err != nil {
		l.Fatal("listing systems failed", log.Error(err))
	}
	l.Debug("listed successfully", log.Array("config_dirs", configDirs))

	for _, configDir := range configDirs {
		cs := storage.NewConfigStorage(YT, configPath.Child(configDir), l)
		var configs fetcher.Configs
		err = cs.ReadConfig(ctx, &configs)
		if err != nil {
			l.Error("config reading failed", log.Error(err))
			continue
		}

		l.Debug("raw config read successfully", log.String("config", fmt.Sprintf("%v", configs)), log.String("system", configDir))

		storageFullPath := storagePath.Child(configDir)
		go func() {
			l.Debug("test1")
			err := runStorageFetcher(YT, configs, storageFullPath.String(), l)
			l.Error("storage fetcher failed", log.Error(err))
		}()
	}

	select {}
}

func runStorageFetcher(YT yt.Client, configs fetcher.Configs, tablePath string, l *logzap.Logger) error {
	l.Debug("test2")
	configs.FillInfo(context.Background(), l)

	l.Debug("config reading succeeded", log.String("config", fmt.Sprintf("%v", configs)))

	errs, _ := errgroup.WithContext(context.Background())
	ts := storage.NewTableStorage(YT, ypath.Path(tablePath), l)
	err := ytprof.MigrateTables(YT, ypath.Path(tablePath))
	if err != nil {
		l.Fatal("migration failed", log.Error(err), log.String("table_path", tablePath))
		return err
	}

	l.Debug("migration succeeded", log.String("table_path", tablePath))

	for _, config := range configs.Configs {
		f := fetcher.NewFetcher(YT, config, l, ts, tablePath)
		errs.Go(func() error {
			return f.RunFetcherContinuous()
		})
	}
	return errs.Wait()
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

	return runStorageFetcher(YT, configs, flagTablePath, l)
}
