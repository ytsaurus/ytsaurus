package parser

import (
	"encoding/json"
	"io"
	"strings"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/consts"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/pkg/instanttime"
)

type RawAccessLogRow struct {
	Value string `yson:"value"`
}

type InstantRow struct {
	MaxInstant instanttime.InstantTime `yson:"max_instant"`
	MinInstant instanttime.InstantTime `yson:"min_instant"`
}

type RawLogParser struct {
	ForceClusterName string
	InputTablesNum   int
}

func (r *RawLogParser) InputTypes() []any {
	schemas := make([]any, r.InputTablesNum)
	for i := range schemas {
		schemas[i] = &RawAccessLogRow{}
	}
	return schemas
}

func (r *RawLogParser) OutputTypes() []any {
	return []any{&consts.AccessLogRow{}, &InstantRow{}}
}

func (r *RawLogParser) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	minInstant := instanttime.InstantTime{}
	maxInstant := instanttime.InstantTime{}
	for in.Next() {
		var row RawAccessLogRow
		in.MustScan(&row)

		decoder := json.NewDecoder(strings.NewReader(row.Value))
		for {
			var rowOut consts.AccessLogRow
			if err := decoder.Decode(&rowOut); err == io.EOF {
				break
			} else if err != nil {
				return xerrors.Errorf("failed to decode line: %w", err)
			}
			if time.Time(maxInstant).IsZero() || time.Time(maxInstant).Before(time.Time(rowOut.Instant)) {
				maxInstant = rowOut.Instant
			}
			if time.Time(minInstant).IsZero() || time.Time(minInstant).After(time.Time(rowOut.Instant)) {
				minInstant = rowOut.Instant
			}
			if r.ForceClusterName != "" {
				rowOut.Cluster = r.ForceClusterName
			}
			out[0].MustWrite(&rowOut)
		}
	}
	out[1].MustWrite(&InstantRow{
		MaxInstant: maxInstant,
		MinInstant: minInstant,
	})
	return nil
}
