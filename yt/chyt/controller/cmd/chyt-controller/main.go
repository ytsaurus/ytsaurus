package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/ytlog"
)

var (
	flagLogToStderr bool
	flagLogsDir     string
	flagConfigPath  string
)

var rootCmd = &cobra.Command{}

func init() {
	rootCmd.PersistentFlags().StringVar(&flagLogsDir, "log-dir", "/", "path to the log directory")
	rootCmd.PersistentFlags().BoolVar(&flagLogToStderr, "log-to-stderr", false, "write logs to stderr")
	rootCmd.PersistentFlags().StringVar(&flagConfigPath, "config", "", "path to the yson config")
	err := rootCmd.MarkPersistentFlagRequired("config")
	if err != nil {
		panic(err)
	}
}

func newLogger(name string) *logzap.Logger {
	if flagLogToStderr {
		return StderrLogger()
	}

	l, _, err := ytlog.NewSelfrotate(filepath.Join(flagLogsDir, name+".log"))

	if err != nil {
		panic(err)
	}

	return l
}

func wrapRun(run func() error) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if err := run(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
			os.Exit(1)
		}
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
