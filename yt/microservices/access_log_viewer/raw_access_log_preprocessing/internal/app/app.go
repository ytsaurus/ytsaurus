package app

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytlock"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/config"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/consts"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/merger"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/parser"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/partitioner"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

type App struct {
	c  *config.Config
	l  *zap.Logger
	yc yt.Client
}

func NewApp(config *config.Config, yc yt.Client, logger *zap.Logger) *App {
	return &App{
		c:  config,
		l:  logger,
		yc: yc,
	}
}

func (a *App) Run(ctx context.Context) error {
	if err := checkNodes(ctx, a.l, a.c, a.yc); err != nil {
		ctxlog.Fatal(ctx, a.l, "error checking prerequisite nodes", log.Error(err))
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	lock := ytlock.NewLockOptions(a.yc, a.c.LockPath, ytlock.Options{
		CreateIfMissing: true,
		LockMode:        yt.LockExclusive,
	})

	lost, err := lock.Acquire(ctx)
	defer lock.Release(ctx)
	if err != nil {
		return err
	}
	go func() {
		<-lost
		ctxlog.Debug(ctx, a.l, "lock released")
		cancelCtx()
	}()

	for {
		rerun, err := a.runOnce(ctx)
		if err != nil {
			return err
		}
		if !rerun {
			ctxlog.Info(ctx, a.l, "everything is processed, finishing")
			break
		}
	}
	return nil
}

func (a *App) runOnce(ctx context.Context) (bool, error) {
	ctx = context.WithValue(ctx, consts.SpanIDKey, ytmsvc.RandHexString(8))
	spanID := ctx.Value(consts.SpanIDKey).(string)
	ctxlog.Info(ctx, a.l, "starting iteration span id", log.String("span_id", spanID))

	rerun := false

	rawLogParser := parser.NewParser(a.l, a.yc, &parser.Config{
		WorkPath:            a.c.WorkPath,
		ForceClusterName:    a.c.ForceClusterName,
		AccessLogExportPath: a.c.AccessLogExportPath,
		MaxTablesToProcess:  a.c.MaxInputTables,
	})
	var err error
	if rerun, err = rawLogParser.Run(ctx); err != nil {
		return false, xerrors.Errorf("failed to parse raw logs: %w", err)
	}

	parsedLogPartitioner := partitioner.NewPartitioner(a.l, a.yc, &partitioner.Config{
		WorkPath:    a.c.WorkPath,
		Processings: a.c.Processings,
	})
	if err := parsedLogPartitioner.Run(ctx); err != nil {
		return false, xerrors.Errorf("failed to partition parsed logs: %w", err)
	}

	partitionsMerger := merger.NewMerger(a.l, a.yc, &merger.Config{
		WorkPath:    a.c.WorkPath,
		Processings: a.c.Processings,
	})
	if err := partitionsMerger.Run(ctx); err != nil {
		return false, xerrors.Errorf("failed to merge partitions: %w", err)
	}

	return rerun, nil
}

func checkNodes(ctx context.Context, logger *zap.Logger, config *config.Config, yc yt.Client) error {
	for _, processing := range config.Processings {
		ctxlog.Debug(ctx, logger, "checking output node", log.String("path", processing.OutputPath.String()))
		exists, err := yc.NodeExists(ctx, processing.OutputPath, &yt.NodeExistsOptions{})
		if err != nil {
			return xerrors.Errorf("failed to check output node %q: %w", processing.OutputPath.String(), err)
		}
		if !exists {
			return xerrors.Errorf("output node %q does not exist", processing.OutputPath.String())
		}
	}
	exists, err := yc.NodeExists(ctx, config.WorkPath, &yt.NodeExistsOptions{})
	if err != nil {
		return xerrors.Errorf("failed to check work node %q: %w", config.WorkPath.String(), err)
	}
	if !exists {
		return xerrors.Errorf("work node %q does not exist", config.WorkPath.String())
	}
	return nil
}
