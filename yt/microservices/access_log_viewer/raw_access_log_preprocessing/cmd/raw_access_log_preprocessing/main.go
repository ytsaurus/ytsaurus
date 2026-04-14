package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/app"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/config"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/parser"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/partitioner"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func init() {
	mapreduce.Register(&parser.RawLogParser{})
	mapreduce.Register(&partitioner.PartitioningMapper{})
}

func newLogger() (*zap.Logger, error) {
	logLevel := os.Getenv("YT_LOG_LEVEL")
	if logLevel == "" {
		logLevel = log.DebugString
	}
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse log level: %w", err)
	}
	logger, err := zap.New(zap.ConsoleConfig(level))
	if err != nil {
		return nil, xerrors.Errorf("failed to create logger: %w", err)
	}
	return logger, nil
}

func start() error {
	ctx := context.Background()

	configPath := flag.String("config", "", "path to the yaml config")
	flag.Parse()

	logger, err := newLogger()
	if err != nil {
		return err
	}

	config, err := config.ReadConfig(*configPath)
	if err != nil {
		return xerrors.Errorf("failed to read config: %w", err)
	}
	if config == nil {
		return xerrors.New("config is nil")
	}
	if err := config.Validate(); err != nil {
		return xerrors.Errorf("failed to validate config: %w", err)
	}

	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:  config.Proxy,
		Logger: logger,
		Token:  ytmsvc.TokenFromEnvVariable(config.TokenEnvVariable),
	})
	if err != nil {
		return xerrors.Errorf("failed to create yt client: %w", err)
	}

	runner := app.NewApp(config, yc, logger)
	return runner.Run(ctx)
}

func main() {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}
	if err := start(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
}
