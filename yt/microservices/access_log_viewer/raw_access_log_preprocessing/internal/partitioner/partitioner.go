package partitioner

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/config"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/consts"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/pkg/instanttime"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

type Partitioner struct {
	l  log.Logger
	yc yt.Client
	c  *Config
}

type Config struct {
	WorkPath    ypath.Path
	Processings map[string]*config.Processing
}

func NewPartitioner(l log.Logger, yc yt.Client, c *Config) *Partitioner {
	return &Partitioner{
		l:  l,
		yc: yc,
		c:  c,
	}
}

func (p *Partitioner) Run(ctx context.Context) error {
	spanID := ctx.Value(consts.SpanIDKey).(string)
	ctxlog.Info(ctx, p.l, "partitioning parsed logs", log.String("span_id", spanID))

	inputTables, err := p.getParsedTables(ctx)
	if err != nil {
		return err
	}
	ctxlog.Info(ctx, p.l, "found tables for partitioning", log.Int("table_count", len(inputTables)))

	for _, parsedTable := range inputTables {
		ctxlog.Debug(ctx, p.l, "partitioning table", log.String("input_table", parsedTable.Path.YPath().String()))
		tx, err := p.yc.BeginTx(ctx, nil)
		if err != nil {
			return xerrors.Errorf("failed to begin transaction: %w", err)
		}
		defer func() { _ = tx.Abort() }()

		if err := p.partitionTable(ctx, tx, parsedTable); err != nil {
			return xerrors.Errorf("failed to partition table: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return xerrors.Errorf("failed to commit transaction: %w", err)
		}
	}

	return nil
}

type parsedTable struct {
	Name         string      `yson:",value"`
	CreationTime yson.Time   `yson:"creation_time,attr"`
	MinTimestamp yson.Time   `yson:"min_timestamp,attr"`
	MaxTimestamp yson.Time   `yson:"max_timestamp,attr"`
	Path         ypath.YPath `yson:"-"`
}

func (p *Partitioner) getParsedTables(ctx context.Context) ([]*parsedTable, error) {
	parsedTablesParent := p.c.WorkPath.Child(consts.ParsedNodeName)

	var tables []*parsedTable
	attributes := []string{
		consts.CreationTimeAttribute,
		consts.MinTimestampAttribute,
		consts.MaxTimestampAttribute,
	}

	if err := p.yc.ListNode(ctx, parsedTablesParent, &tables, &yt.ListNodeOptions{
		Attributes: attributes,
	}); err != nil {
		return nil, xerrors.Errorf("failed to list tables: %w", err)
	}

	sort.Slice(tables, func(i, j int) bool {
		return time.Time(tables[i].CreationTime).Before(time.Time(tables[j].CreationTime))
	})

	for tableIndex := range tables {
		tables[tableIndex].Path = parsedTablesParent.Child(tables[tableIndex].Name)
	}
	return tables, nil
}

func (p *Partitioner) partitionTable(ctx context.Context, tx yt.Tx, table *parsedTable) error {
	spanID, _ := ctx.Value(consts.SpanIDKey).(string)

	var partitions []*partition
	var outputTables []ypath.YPath

	partitionsParent := p.c.WorkPath.Child(consts.PartitionsNodeName)

	for processingName, processingConfig := range p.c.Processings {
		minPartitionIndex := time.Time(table.MinTimestamp).Unix() / int64(processingConfig.Interval.Seconds())
		maxPartitionIndex := time.Time(table.MaxTimestamp).Unix() / int64(processingConfig.Interval.Seconds())
		for partitionIndex := minPartitionIndex; partitionIndex <= maxPartitionIndex; partitionIndex++ {
			minTimestamp := instanttime.InstantTime(time.Unix(partitionIndex*int64(processingConfig.Interval.Seconds()), 0).UTC())
			maxTimestamp := instanttime.InstantTime(time.Unix((partitionIndex+1)*int64(processingConfig.Interval.Seconds()), 0).UTC())
			partitions = append(partitions, &partition{
				MinTimestamp: minTimestamp,
				MaxTimestamp: maxTimestamp,
			})
			outputTables = append(outputTables, partitionsParent.Child(processingName).Child(time.Time(minTimestamp).Format(processingConfig.TableFormat)).Child(ytmsvc.RandHexString(8)))
		}
	}

	mapSpec := spec.Map()
	mapSpec.SortBy = consts.ParsedTableSchema.KeyColumns()
	mapSpec.InputTablePaths = []ypath.YPath{table.Path}
	mapSpec.OutputTablePaths = outputTables
	for _, outputTable := range outputTables {
		if _, err := tx.CreateNode(ctx, outputTable, yt.NodeTable, &yt.CreateNodeOptions{
			Attributes: map[string]any{
				"schema": consts.ParsedTableSchema,
			},
			Recursive: true,
		}); err != nil {
			return xerrors.Errorf("failed to create output table: %w", err)
		}
	}

	mapSpec.Title = fmt.Sprintf("[Raw Access Log Preprocessing] Processing time partitions [%s]", spanID)
	mapSpec.Ordered = true

	mr := mapreduce.New(p.yc).WithTx(tx)
	mapOp, err := mr.Map(&PartitioningMapper{Partitions: partitions, OutputTableCount: len(outputTables)}, mapSpec)
	if err != nil {
		return xerrors.Errorf("failed to create map operation: %w", err)
	}

	if err := mapOp.Wait(); err != nil {
		return xerrors.Errorf("failed to wait for map operation: %w", err)
	}

	if err := tx.RemoveNode(ctx, table.Path, &yt.RemoveNodeOptions{}); err != nil {
		return xerrors.Errorf("failed to remove input table: %w", err)
	}

	return nil
}
