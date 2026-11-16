package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type accessLogInputTable struct {
	Path          ypath.Path
	IntervalBegin time.Time
	IntervalEnd   time.Time
	SortedByPath  bool
}

type accessLogTableListItem struct {
	Name     string      `yson:",value"`
	ID       yt.NodeID   `yson:"id,attr"`
	Type     yt.NodeType `yson:"type,attr"`
	Dynamic  bool        `yson:"dynamic,attr"`
	SortedBy []string    `yson:"sorted_by,attr"`
}

// listAccessLogTables lists the static tables of one period and pins them by object ID.
func listAccessLogTables(
	ctx context.Context,
	client yt.Client,
	directory ypath.Path,
	period string,
	logsLocation *time.Location,
) ([]accessLogInputTable, error) {
	var tableNameLayout string
	switch period {
	case "1d":
		tableNameLayout = "2006-01-02"
	case "30min":
		tableNameLayout = "2006-01-02T15:04:05"
	default:
		return nil, fmt.Errorf("unsupported access log period %q", period)
	}
	var children []accessLogTableListItem
	if err := client.ListNode(ctx, directory, &children, &yt.ListNodeOptions{Attributes: []string{"id", "type", "dynamic", "sorted_by"}}); err != nil {
		return nil, yterrors.Err("could not list access log input tables", yterrors.Attr("path", directory.String()), err)
	}
	tables := make([]accessLogInputTable, 0, len(children))
	for _, child := range children {
		if child.Type != yt.NodeTable || child.Dynamic {
			continue
		}
		intervalBegin, err := time.ParseInLocation(tableNameLayout, child.Name, logsLocation)
		if err != nil {
			continue
		}
		intervalEnd := intervalBegin.Add(30 * time.Minute)
		if period == "1d" {
			intervalEnd = intervalBegin.AddDate(0, 0, 1)
		}
		tables = append(tables, accessLogInputTable{
			Path:          child.ID.YPath(),
			IntervalBegin: intervalBegin,
			IntervalEnd:   intervalEnd,
			SortedByPath:  len(child.SortedBy) > 0 && child.SortedBy[0] == "path",
		})
	}
	return tables, nil
}

func (e *AccessLogExporter) listAccessLogInputTables(ctx context.Context, cluster string) ([]accessLogInputTable, error) {
	var candidates []accessLogInputTable
	var missingDirectoryError error
	listedDirectory := false
	for _, period := range []string{"1d", "30min"} {
		directory := e.config.SnapshotRoot.Child(cluster).Child(period)
		tables, err := listAccessLogTables(ctx, e.operationClusterClient, directory, period, e.logsLocation)
		if yterrors.ContainsResolveError(err) {
			missingDirectoryError = err
			continue
		}
		if err != nil {
			return nil, err
		}
		listedDirectory = true
		candidates = append(candidates, tables...)
	}
	if !listedDirectory {
		return nil, fmt.Errorf("access log snapshots are missing on operation cluster %q: %w", e.config.OperationCluster, missingDirectoryError)
	}
	return candidates, nil
}

// selectAccessLogInputTables selects daily tables overlapping [begin, end) and covers the rest with half-hour
// tables. A zero end means the end of the latest available table.
func (e *AccessLogExporter) selectAccessLogInputTables(ctx context.Context, cluster string, begin, end int64) ([]accessLogInputTable, int64, error) {
	candidates, err := e.listAccessLogInputTables(ctx, cluster)
	if err != nil {
		return nil, 0, yterrors.Err("access log export input tables are unavailable", err)
	}
	return selectInputCandidates(candidates, begin, end, e.config.MaxInputTableCount)
}

func selectInputCandidates(candidates []accessLogInputTable, begin, end int64, maxTableCount int) ([]accessLogInputTable, int64, error) {
	if end == 0 {
		for _, candidate := range candidates {
			if tableEnd := candidate.IntervalEnd.Unix(); tableEnd > begin && tableEnd > end {
				end = tableEnd
			}
		}
		if end == 0 {
			return nil, 0, yterrors.Err("there are no access log tables after begin", yterrors.Attr("begin", begin))
		}
	}

	var overlapping []accessLogInputTable
	for _, candidate := range candidates {
		if candidate.IntervalEnd.Unix() > begin && candidate.IntervalBegin.Unix() < end {
			overlapping = append(overlapping, candidate)
		}
	}
	// A daily table goes before the half-hour tables of the same day.
	sort.Slice(overlapping, func(i, j int) bool {
		if overlapping[i].IntervalBegin.Equal(overlapping[j].IntervalBegin) {
			return overlapping[i].IntervalEnd.After(overlapping[j].IntervalEnd)
		}
		return overlapping[i].IntervalBegin.Before(overlapping[j].IntervalBegin)
	})

	coveredUntil := begin
	var selected []accessLogInputTable
	for _, candidate := range overlapping {
		tableBegin, tableEnd := candidate.IntervalBegin.Unix(), candidate.IntervalEnd.Unix()
		if tableEnd <= coveredUntil {
			continue
		}
		if tableBegin > coveredUntil {
			return nil, 0, yterrors.Err(
				"access log interval contains a hole",
				yterrors.Attr("covered_until", coveredUntil),
				yterrors.Attr("next_table_begin", tableBegin),
			)
		}
		if len(selected) > 0 && tableBegin < coveredUntil {
			return nil, 0, yterrors.Err(
				"access log tables overlap without containment",
				yterrors.Attr("covered_until", coveredUntil),
				yterrors.Attr("next_table_begin", tableBegin),
			)
		}
		selected = append(selected, candidate)
		coveredUntil = tableEnd
		if coveredUntil >= end {
			break
		}
	}
	if coveredUntil < end {
		return nil, 0, yterrors.Err(
			"access log tables do not cover the requested interval",
			yterrors.Attr("begin", begin),
			yterrors.Attr("end", end),
			yterrors.Attr("covered_until", coveredUntil),
		)
	}
	if len(selected) > maxTableCount {
		return nil, 0, yterrors.Err(
			"too many input tables for one export operation",
			yterrors.Attr("table_count", len(selected)),
			yterrors.Attr("limit", maxTableCount),
		)
	}
	return selected, end, nil
}

// inputTablePathsLimitedToPathKeyRange limits the tables sorted by path to the key range [path, path+"0") unless the
// whole Cypress is exported. "&" and "/" precede "0" in byte order, so the range contains the requested path, its
// attributes, and its descendants. It also contains siblings such as //a/b-c for //a/b, which the mapper filters out.
func inputTablePathsLimitedToPathKeyRange(tables []accessLogInputTable, path string) []ypath.YPath {
	inputTablePaths := make([]ypath.YPath, 0, len(tables))
	for _, table := range tables {
		if path == "/" || !table.SortedByPath {
			inputTablePaths = append(inputTablePaths, table.Path)
			continue
		}
		inputTablePaths = append(inputTablePaths, &ypath.Rich{
			Path:   table.Path,
			Ranges: []ypath.Range{ypath.Interval(ypath.Key(path), ypath.Key(path+"0"))},
		})
	}
	return inputTablePaths
}
