package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"gopkg.in/yaml.v2"

	"go.ytsaurus.tech/library/go/core/log"
	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/ytlog"
	lib "go.ytsaurus.tech/yt/microservices/lib/go"
	"go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/app"
)

func main() {
	AdjustMaxProcs()

	logToStderr := flag.Bool("log-to-stderr", false, "write logs to stderr")
	logDir := flag.String("log-dir", "", "log output directory")
	configPath := flag.String("config", "", "path to the yaml config")
	flag.Parse()

	logger, stop := newLogger(*logToStderr, *logDir)
	defer stop()
	lib.Logger = logger

	conf, err := readConfig(*configPath)
	if err != nil {
		logger.Fatalf("unable to read config from %s: %s", *configPath, err)
	}

	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return waitSignal(ctx, logger)
	})

	g.Go(ensureError(func() error {
		err := app.NewApp(conf, logger).Run(ctx)
		logger.Info("app stopped", log.Error(err))
		return err
	}))

	if err := g.Wait(); err != nil {
		logger.Info("main stopped", log.Error(err))
	}
}

func readConfig(path string) (*app.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var conf app.Config
	err = yaml.Unmarshal(data, &conf)
	return &conf, err
}

func newLogger(logToStderr bool, logDir string) (*logzap.Logger, func()) {
	if logDir == "" {
		return ytlog.Must(), func() {}
	}

	_ = os.Mkdir(logDir, 0755)

	debugL, debugStop, err := ytlog.NewSelfrotate(filepath.Join(logDir, "resource-usage.debug.log"))
	if err != nil {
		panic(err)
	}
	errorL, errorStop, err := ytlog.NewSelfrotate(filepath.Join(logDir, "resource-usage.error.log"), ytlog.WithLogLevel(zapcore.ErrorLevel))
	if err != nil {
		panic(err)
	}

	combinedCore := zapcore.NewTee(debugL.L.Core(), errorL.L.Core())

	if logToStderr {
		combinedCore = zapcore.NewTee(combinedCore, ytlog.Must().L.Core())
	}

	l := logzap.NewWithCore(combinedCore)

	stop := func() {
		debugStop()
		errorStop()
	}

	return l, stop
}

func waitSignal(ctx context.Context, l *logzap.Logger) error {
	exitSignal := make(chan os.Signal, 1)
	signal.Notify(exitSignal, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-exitSignal:
		l.Info("received signal", log.String("signal", s.String()))
		return xerrors.New("interrupted")
	case <-ctx.Done():
		return nil
	}
}

func ensureError(f func() error) func() error {
	return func() error {
		if err := f(); err != nil {
			return err
		}
		return xerrors.Errorf("unexpected nil")
	}
}
