package merger

import (
	"context"
	"fmt"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/config"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/consts"
)

type Merger struct {
	l  log.Logger
	yc yt.Client
	c  *Config
}

type Config struct {
	WorkPath    ypath.Path
	Processings map[string]*config.Processing
}

func NewMerger(l log.Logger, yc yt.Client, c *Config) *Merger {
	return &Merger{
		l:  l,
		yc: yc,
		c:  c,
	}
}

func (m *Merger) Run(ctx context.Context) error {
	spanID := ctx.Value(consts.SpanIDKey).(string)
	ctxlog.Info(ctx, m.l, "starting merge", log.String("span_id", spanID))

	partitionsParent := m.c.WorkPath.Child(consts.PartitionsNodeName)
	for processingName, processingConfig := range m.c.Processings {
		var partitions []string
		if err := m.yc.ListNode(ctx, partitionsParent.Child(processingName), &partitions, &yt.ListNodeOptions{}); err != nil {
			return xerrors.Errorf("failed to list partitions: %w", err)
		}
		for _, partition := range partitions {
			if ready, err := m.partitionReadyToMerge(ctx, partition, processingConfig); err != nil {
				return xerrors.Errorf("failed to check if partition %q ready to merge: %w", partition, err)
			} else if !ready {
				continue
			}
			tx, err := m.yc.BeginTx(ctx, nil)
			if err != nil {
				return xerrors.Errorf("failed to begin transaction: %w", err)
			}
			defer func() { _ = tx.Abort() }()
			if err := m.mergePartition(ctx, tx, processingName, partition); err != nil {
				return xerrors.Errorf("failed to merge partition %q: %w", partition, err)
			}
			if err := tx.RemoveNode(ctx, partitionsParent.Child(processingName).Child(partition), &yt.RemoveNodeOptions{Recursive: true}); err != nil {
				return xerrors.Errorf("failed to remove chunks: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return xerrors.Errorf("failed to commit transaction: %w", err)
			}
		}
	}
	return nil
}

func (m *Merger) mergePartition(ctx context.Context, tx yt.Tx, processingName string, partition string) error {
	chunksParent := m.c.WorkPath.Child(consts.PartitionsNodeName).Child(processingName).Child(partition)

	type chunk struct {
		Name     string `yson:",value"`
		RowCount int    `yson:"row_count,attr"`
	}

	var chunks []chunk

	if err := m.yc.ListNode(ctx, chunksParent, &chunks, &yt.ListNodeOptions{Attributes: []string{"row_count"}}); err != nil {
		return xerrors.Errorf("failed to list chunks: %w", err)
	}
	var inputTablePaths []ypath.YPath
	for _, chunk := range chunks {
		if chunk.RowCount == 0 {
			continue
		}
		inputTablePaths = append(inputTablePaths, chunksParent.Child(chunk.Name))
	}

	outputPath := m.c.Processings[processingName].OutputPath.Child(partition)

	if ok, err := m.yc.NodeExists(ctx, outputPath, &yt.NodeExistsOptions{}); err != nil {
		return xerrors.Errorf("failed to check if output table exists: %w", err)
	} else if ok {
		inputTablePaths = append(inputTablePaths, outputPath)
	} else {
		if _, err := tx.CreateNode(ctx, outputPath, yt.NodeTable, &yt.CreateNodeOptions{
			Recursive:  true,
			Attributes: map[string]any{"schema": consts.ParsedTableSchema},
		}); err != nil {
			return xerrors.Errorf("failed to create output table: %w", err)
		}
	}

	if len(inputTablePaths) == 0 {
		return nil
	}
	if len(inputTablePaths) == 1 {
		if inputTablePaths[0] == outputPath {
			return nil
		}
		if _, err := tx.MoveNode(ctx, inputTablePaths[0], outputPath, &yt.MoveNodeOptions{Force: true}); err != nil {
			return xerrors.Errorf("failed to move input table: %w", err)
		}
		return nil
	}

	sortSpec := spec.Sort()
	sortSpec.SortBy = consts.ParsedTableSchema.KeyColumns()
	sortSpec.InputTablePaths = inputTablePaths
	sortSpec.OutputTablePath = outputPath

	spanID := ctx.Value(consts.SpanIDKey).(string)
	sortSpec.Title = fmt.Sprintf("[Raw Access Log Preprocessing] Processing time partitions [%s]", spanID)

	mr := mapreduce.New(m.yc).WithTx(tx)
	sortOp, err := mr.Sort(sortSpec)
	if err != nil {
		return xerrors.Errorf("failed to create sort operation: %w", err)
	}

	if err := sortOp.Wait(); err != nil {
		return xerrors.Errorf("failed to wait for sort operation: %w", err)
	}

	return nil
}

func (m *Merger) partitionReadyToMerge(ctx context.Context, partition string, processingConfig *config.Processing) (bool, error) {
	partTimeStart, err := time.Parse(processingConfig.TableFormat, partition)
	if err != nil {
		return false, xerrors.Errorf("failed to parse partition %q using layout %q: %w", partition, processingConfig.TableFormat, err)
	}
	partTimeEnd := partTimeStart.Add(processingConfig.Interval)

	if time.Now().UTC().After(partTimeEnd.Add(processingConfig.CutoffTime)) {
		ctxlog.Debug(ctx, m.l, "partition is ready for merge due to `time > part_end + cutoff`", log.Any("partition", partition))
		return true, nil
	}

	return false, nil
}
