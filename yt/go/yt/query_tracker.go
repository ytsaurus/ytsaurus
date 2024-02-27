package yt

import (
	"context"
	"time"

	"golang.org/x/exp/slices"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yterrors"
)

const (
	defaultTrackPollPeriod = 10 * time.Second
)

type (
	QueryResult struct {
		ID             QueryID          `yson:"id"`
		ResultIndex    int64            `yson:"result_index"`
		Err            yterrors.Error   `yson:"error"`
		Schema         []map[string]any `yson:"schema"`
		DataStatistics DataStatistics   `yson:"data_statistics"`
	}

	Query struct {
		ID              QueryID        `yson:"id"`
		Engine          *QueryEngine   `yson:"engine"`
		Query           *string        `yson:"query"`
		StartTime       *yson.Time     `yson:"start_time"`
		FinishTime      *yson.Time     `yson:"finish_time"`
		Settings        any            `yson:"settings"`
		User            *string        `yson:"user"`
		State           *QueryState    `yson:"state"`
		ResultCount     *int64         `yson:"result_count"`
		Progress        any            `yson:"progress"`
		Err             yterrors.Error `yson:"error"`
		Annotations     any            `yson:"annotations"`
		OtherAttributes any            `yson:"other_attributes"`
	}

	ListQueriesResult struct {
		Queries    []Query `yson:"queries"`
		Incomplete bool    `yson:"incomplete"`
		Timestamp  uint64  `yson:"timestamp"`
	}

	DataStatistics struct {
		UncompressedDataSize int64 `yson:"uncompressed_data_size"`
		CompressedDataSize   int64 `yson:"compressed_data_size"`
		RowCount             int64 `yson:"row_count"`
		ChunkCount           int64 `yson:"chunk_count"`
		RegularDiskSpace     int64 `yson:"regular_disk_space"`
		ErasureDiskSpace     int64 `yson:"erasure_disk_space"`

		// For backward compatibility this can be -1 which means "invalid value".
		DataWeight int64 `yson:"data_weight"`

		UnmergedRowCount   int64 `yson:"unmerged_row_count"`
		UnmergedDataWeight int64 `yson:"unmerged_data_weight"`
	}
)

type TrackQueryOptions struct {
	Logger     log.Structured
	PollPeriod *time.Duration
	*QueryTrackerOptions
}

// TrackQuery is waiting while Query will get state == "completed" | "failed" | "aborted".
//
// If the opts.PollPeriod is not set, then pollPeriod = defaultTrackPollPeriod.
// If the opts.Logger is not set, then Logger = nop.Logger
func TrackQuery(
	ctx context.Context,
	qt QueryTrackerClient,
	id QueryID,
	opts *TrackQueryOptions,
) error {
	if opts == nil {
		opts = &TrackQueryOptions{}
	}
	if opts.PollPeriod == nil {
		opts.PollPeriod = ptr.Duration(defaultTrackPollPeriod)
	}
	if opts.Logger == nil {
		opts.Logger = &nop.Logger{}
	}

	checkQueryState := func() (bool, error) {
		query, err := qt.GetQuery(ctx, id, &GetQueryOptions{
			Attributes:          []string{"state", "error"},
			QueryTrackerOptions: opts.QueryTrackerOptions,
		})
		if err != nil {
			return false, err
		}

		state := query.State
		if state == nil {
			return false, nil
		}

		opts.Logger.Debug("Query", log.Any("query_id", id), log.Any("state", *state))

		if slices.Contains([]QueryState{QueryStateFailed, QueryStateAborted}, *state) {
			return false, query.Err.Unwrap()
		}
		if *state == QueryStateCompleted {
			return true, nil
		}
		return false, nil
	}

	t := time.NewTicker(*opts.PollPeriod)
	defer t.Stop()

	for {
		completed, err := checkQueryState()
		if yterrors.ContainsErrorCode(err, yterrors.CodeQueryNotFound) {
			return err
		}
		if completed {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}
