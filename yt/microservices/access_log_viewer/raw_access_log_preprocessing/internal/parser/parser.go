package parser

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
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/consts"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/pkg/instanttime"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

type Parser struct {
	l  log.Logger
	yc yt.Client
	c  *Config
}

type Config struct {
	WorkPath            ypath.Path
	AccessLogExportPath ypath.Path
	ForceClusterName    string
	MaxTablesToProcess  int
}

func NewParser(l log.Logger, yc yt.Client, c *Config) *Parser {
	return &Parser{
		l:  l,
		yc: yc,
		c:  c,
	}
}

func (p *Parser) Run(ctx context.Context) (bool, error) {
	spanID := ctx.Value(consts.SpanIDKey).(string)
	ctxlog.Info(ctx, p.l, "starting parse raw access logs", log.String("span_id", spanID))

	inputTables, err := p.getRawTables(ctx)
	if err != nil {
		return false, xerrors.Errorf("failed to get input tables: %w", err)
	}
	if len(inputTables) == 0 {
		ctxlog.Info(ctx, p.l, "no input tables found")
		return false, nil
	}
	ctxlog.Info(ctx, p.l, "found tables to parse", log.Int("tables_num", len(inputTables)))

	tx, err := p.yc.BeginTx(ctx, &yt.StartTxOptions{})
	if err != nil {
		return false, xerrors.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Abort() }()

	parsedUnsortedTable := p.c.WorkPath.Child("tmp").Child(spanID).Child(consts.ParsedUnsortedNodeName)
	if _, err := tx.CreateNode(ctx, parsedUnsortedTable, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{"schema": consts.ParsedTableSchema.SortedBy()},
		Recursive:  true,
	}); err != nil {
		return false, xerrors.Errorf("failed to create parsed unsorted table: %w", err)
	}

	timestampsTable := p.c.WorkPath.Child("tmp").Child(spanID).Child("timestamps")
	if _, err := tx.CreateNode(ctx, timestampsTable, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{"schema": consts.InstantsTableSchema},
	}); err != nil {
		return false, xerrors.Errorf("failed to timestamps table: %w", err)
	}

	mapSpec := spec.Map()
	mapSpec.InputTablePaths = inputTables
	mapSpec.OutputTablePaths = []ypath.YPath{parsedUnsortedTable, timestampsTable}
	mapSpec.Title = fmt.Sprintf("[Raw Access Log Preprocessing] Parsing raw access logs [%s]", spanID)

	mr := mapreduce.New(p.yc).WithTx(tx)
	op, err := mr.Map(&RawLogParser{ForceClusterName: p.c.ForceClusterName, InputTablesNum: len(inputTables)}, mapSpec)
	if err != nil {
		return false, xerrors.Errorf("failed to start map: %w", err)
	}

	if err := op.Wait(); err != nil {
		return false, xerrors.Errorf("failed to wait for map %q: %w", op.ID().String(), err)
	}

	minTimestamp, maxTimestamp, err := p.getTimestampRange(ctx, tx, timestampsTable)
	if err != nil {
		return false, xerrors.Errorf("failed to get timestamp range: %w", err)
	}

	parsedTable := p.c.WorkPath.Child(consts.ParsedNodeName).Child(fmt.Sprintf("%015d_%s", time.Time(minTimestamp).UnixMilli(), ytmsvc.RandHexString(8)))
	if _, err := tx.CreateNode(ctx, parsedTable, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"schema":                     consts.ParsedTableSchema,
			consts.MinTimestampAttribute: yson.Time(minTimestamp),
			consts.MaxTimestampAttribute: yson.Time(maxTimestamp),
		},
		Recursive: true,
	}); err != nil {
		return false, xerrors.Errorf("failed to create parsed table: %w", err)
	}

	sortSpec := spec.Sort()
	sortSpec = sortSpec.AddInput(parsedUnsortedTable)
	sortSpec = sortSpec.SetOutput(parsedTable)
	sortSpec = sortSpec.SortByColumns(consts.ParsedTableSchema.KeyColumns()...)
	sortSpec.Title = fmt.Sprintf("[Raw Access Log Preprocessing] Sorting parsed access logs [%s]", spanID)

	sortOp, err := mr.Sort(sortSpec)
	if err != nil {
		return false, xerrors.Errorf("failed to start sort: %w", err)
	}

	if err = sortOp.Wait(); err != nil {
		return false, xerrors.Errorf("failed to wait for sort %q: %w", sortOp.ID().String(), err)
	}

	if err := tx.RemoveNode(ctx, p.c.WorkPath.Child("tmp").Child(spanID), &yt.RemoveNodeOptions{Recursive: true}); err != nil {
		return false, xerrors.Errorf("failed to remove tmp table: %w", err)
	}

	for _, inputTable := range inputTables {
		if err := tx.SetNode(ctx, inputTable.YPath().Attr(consts.ProcessedAtAttribute), yson.Time(time.Now().UTC()), &yt.SetNodeOptions{}); err != nil {
			return false, xerrors.Errorf("failed to set processed attribute: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return false, xerrors.Errorf("failed to commit transaction: %w", err)
	}
	ctxlog.Info(ctx, p.l, "finished parsing raw access logs", log.String("span_id", spanID), log.String("parsed_table", parsedTable.String()))

	return true, nil
}

func (p *Parser) getRawTables(ctx context.Context) ([]ypath.YPath, error) {
	type rawTable struct {
		Name         string    `yson:",value"`
		CreationTime time.Time `yson:"creation_time,attr"`
		ProcessedAt  time.Time `yson:"alv_processed_at,attr"`
	}

	var tables []*rawTable
	attributes := []string{consts.CreationTimeAttribute, consts.ProcessedAtAttribute}

	if err := p.yc.ListNode(ctx, p.c.AccessLogExportPath, &tables, &yt.ListNodeOptions{Attributes: attributes}); err != nil {
		return nil, xerrors.Errorf("failed to list tables: %w", err)
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].CreationTime.Before(tables[j].CreationTime)
	})

	tablesToParse := []ypath.YPath{}
	for _, table := range tables {
		if !table.ProcessedAt.IsZero() {
			continue
		}
		tablesToParse = append(tablesToParse, p.c.AccessLogExportPath.Child(table.Name))
		if len(tablesToParse) >= p.c.MaxTablesToProcess {
			break
		}
	}
	return tablesToParse, nil
}

func (p *Parser) getTimestampRange(ctx context.Context, tx yt.Tx, timestampsTable ypath.YPath) (instanttime.InstantTime, instanttime.InstantTime, error) {
	r, err := tx.ReadTable(ctx, timestampsTable, &yt.ReadTableOptions{Unordered: true})
	if err != nil {
		return instanttime.InstantTime{}, instanttime.InstantTime{}, xerrors.Errorf("failed to read table %q: %w", timestampsTable.YPath().String(), err)
	}
	defer r.Close()

	var maxInstant, minInstant instanttime.InstantTime

	for r.Next() {
		var row InstantRow
		if err = r.Scan(&row); err != nil {
			return instanttime.InstantTime{}, instanttime.InstantTime{}, xerrors.Errorf("failed to scan row: %w", err)
		}
		if time.Time(maxInstant).IsZero() || time.Time(row.MaxInstant).After(time.Time(maxInstant)) {
			maxInstant = row.MaxInstant
		}
		if time.Time(minInstant).IsZero() || time.Time(row.MinInstant).Before(time.Time(minInstant)) {
			minInstant = row.MinInstant
		}
	}

	return minInstant, maxInstant, nil
}
