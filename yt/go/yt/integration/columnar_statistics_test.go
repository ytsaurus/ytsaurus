package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestGetTableColumnarStatistics(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "GetTableColumnarStatistics", Test: suite.TestGetTableColumnarStatistics, SkipRPC: true},
	})
}

type columnarStatisticsRow struct {
	Filled  string  `yson:"filled"`
	Partial *string `yson:"partial"`
	Empty   *string `yson:"empty"`
}

func (s *Suite) TestGetTableColumnarStatistics(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	const (
		rowCount     = 10
		filledValue  = "filled-value"
		partialValue = "partial"
	)

	path := tmpPath()
	_, err := yc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"schema": schema.MustInfer(&columnarStatisticsRow{}),
		},
	})
	require.NoError(t, err)

	// "filled" is set in every row, "partial" in every other row, "empty" is never set.
	w, err := yc.WriteTable(ctx, path, nil)
	require.NoError(t, err)
	for i := 0; i < rowCount; i++ {
		row := columnarStatisticsRow{Filled: filledValue}
		if i%2 == 0 {
			row.Partial = ptr.String(partialValue)
		}
		require.NoError(t, w.Write(row))
	}
	require.NoError(t, w.Commit())

	partialCount := int64(rowCount / 2)

	t.Run("AllColumns", func(t *testing.T) {
		stats, err := yc.GetTableColumnarStatistics(ctx, []ypath.YPath{path}, nil)
		require.NoError(t, err)
		require.Len(t, stats, 1)

		require.Equal(t, map[string]int64{
			"filled":  int64(rowCount * len(filledValue)),
			"partial": partialCount * int64(len(partialValue)),
			"empty":   0,
		}, stats[0].ColumnDataWeights)

		require.Equal(t, map[string]int64{
			"filled":  rowCount,
			"partial": partialCount,
			"empty":   0,
		}, stats[0].ColumnNonNullValueCounts)

		require.NotNil(t, stats[0].ChunkRowCount)
		require.Equal(t, int64(rowCount), *stats[0].ChunkRowCount)
		require.Equal(t, int64(0), stats[0].LegacyChunksDataWeight)
	})

	t.Run("ColumnSelector", func(t *testing.T) {
		rich := ypath.NewRich(path.String()).SetColumns([]string{"empty", "filled"})

		stats, err := yc.GetTableColumnarStatistics(ctx, []ypath.YPath{rich, path}, nil)
		require.NoError(t, err)
		require.Len(t, stats, 2)

		require.Equal(t, map[string]int64{
			"filled": int64(rowCount * len(filledValue)),
			"empty":  0,
		}, stats[0].ColumnDataWeights)
		require.Equal(t, map[string]int64{
			"filled": rowCount,
			"empty":  0,
		}, stats[0].ColumnNonNullValueCounts)

		// Second path has no column selector and reports all schema columns.
		require.Len(t, stats[1].ColumnDataWeights, 3)
		require.Equal(t, stats[0].ColumnDataWeights["filled"], stats[1].ColumnDataWeights["filled"])
	})

	t.Run("Options", func(t *testing.T) {
		stats, err := yc.GetTableColumnarStatistics(ctx, []ypath.YPath{path}, &yt.GetTableColumnarStatisticsOptions{
			FetcherMode:              ptr.T(yt.ColumnarStatisticsFetcherModeFallback),
			MaxChunksPerNodeFetch:    ptr.Int(1),
			EnableEarlyFinish:        ptr.Bool(false),
			EnableReadSizeEstimation: ptr.Bool(true),
		})
		require.NoError(t, err)
		require.Len(t, stats, 1)
		require.Equal(t, int64(rowCount*len(filledValue)), stats[0].ColumnDataWeights["filled"])
	})

	t.Run("MissingTable", func(t *testing.T) {
		_, err := yc.GetTableColumnarStatistics(ctx, []ypath.YPath{tmpPath()}, nil)
		require.Error(t, err)
	})
}
