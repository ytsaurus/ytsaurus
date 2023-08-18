package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/ytlog"
)

const (
	cluster = "pythia"
)

var (
	logger = ytlog.Must()
	stage  = ptr.String(cluster + ":testing")
	path   = ypath.Path("//tmp/query-tracker")
)

type Row struct {
	Key int64  `yson:"key,key"`
	Str string `yson:"str"`
}

func initDynamicTable(ctx context.Context, yc yt.Client) error {
	err := migrate.EnsureTables(ctx, yc, map[ypath.Path]migrate.Table{
		path: {
			Schema: schema.MustInfer(&Row{}),
		},
	}, migrate.OnConflictFail)
	if err != nil {
		return err
	}

	rows := []any{
		Row{Key: 0, Str: "some_str"},
		Row{Key: 1, Str: "another_str"},
	}
	return yc.InsertRows(ctx, path, rows, nil)
}

func removeDynamicTable(ctx context.Context, yc yt.Client) {
	err := yc.RemoveNode(ctx, path, nil)
	if err != nil {
		logger.Error("error on removing dynamic table", log.Error(err))
	}
}

func readSubqueryResult(ctx context.Context, yc yt.Client, id yt.QueryID, resultIndex int64) ([]Row, error) {
	reader, err := yc.ReadQueryResult(ctx, id, resultIndex, &yt.ReadQueryResultOptions{
		Columns: []string{"key"},
		QueryTrackerOptions: &yt.QueryTrackerOptions{
			Stage: stage,
		},
	})
	if err != nil {
		return nil, err
	}

	var rows []Row
	for reader.Next() {
		var row Row
		if err = reader.Scan(&row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	err = reader.Err()
	return rows, err
}

func Example(ctx context.Context, yc yt.Client) error {
	if err := initDynamicTable(ctx, yc); err != nil {
		return err
	}
	defer removeDynamicTable(ctx, yc)

	id, err := yc.StartQuery(ctx, yt.QueryEngineQL, fmt.Sprintf("* from [%s]", path), &yt.StartQueryOptions{
		Settings: map[string]any{
			"cluster": cluster,
		},
		QueryTrackerOptions: &yt.QueryTrackerOptions{
			Stage: stage,
		},
	})
	if err != nil {
		return err
	}
	logger.Info("Query is started", log.Any("id", id))

	err = yt.TrackQuery(ctx, yc, id, &yt.TrackQueryOptions{
		Logger:              logger,
		QueryTrackerOptions: &yt.QueryTrackerOptions{Stage: stage},
	})
	if err != nil {
		return err
	}

	query, err := yc.GetQuery(ctx, id, &yt.GetQueryOptions{
		QueryTrackerOptions: &yt.QueryTrackerOptions{
			Stage: stage,
		},
	})
	if err != nil {
		return err
	}
	logger.Info("Got completed Query")

	for i := int64(0); i < *query.ResultCount; i++ {
		rows, err := readSubqueryResult(ctx, yc, id, i)
		if err != nil {
			return err
		}

		logger.Info("Got QueryResult", log.Int64("result_index", i), log.Any("rows", rows))
	}

	return nil
}

func main() {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:             cluster,
		ReadTokenFromFile: true,
		Logger:            logger,
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	if err := Example(ctx, yc); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
}
