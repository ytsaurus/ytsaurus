package integration

import (
	"fmt"
	"testing"

	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/yt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrderedTables(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "OrderedDynamicTable_struct", Test: suite.TestOrderedDynamicTable_struct},
		{Name: "OrderedDynamicTable_map", Test: suite.TestOrderedDynamicTable_map, SkipRPC: true}, // todo https://st.yandex-team.ru/YT-15505
	})
}

type testOrderedTableRow struct {
	TabletIndex int    `yson:"$tablet_index"`
	RowIndex    int    `yson:"$row_index,omitempty"`
	Value       string `yson:"value"`
}

func (s *Suite) TestOrderedDynamicTable_struct(t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	tableSchema := schema.MustInfer(&testOrderedTableRow{})
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, tableSchema))

	require.NoError(t, yc.ReshardTable(s.Ctx, testTable, &yt.ReshardTableOptions{
		TabletCount: ptr.Int(6),
	}))

	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	rows := []interface{}{&testOrderedTableRow{TabletIndex: 2, Value: "hello"}}
	require.NoError(t, yc.InsertRows(s.Ctx, testTable, rows, nil))

	r, err := yc.SelectRows(s.Ctx, fmt.Sprintf("* from [%s] where [$tablet_index] = 2", testTable), nil)
	require.NoError(t, err)

	var row testOrderedTableRow
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	assert.Equal(t, rows[0], &row)
	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func (s *Suite) TestOrderedDynamicTable_map(t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	tableSchema := schema.MustInferMap(map[string]interface{}{
		"$tablet_index": 1, // has not effect
		"$row_index":    1, // has not effect
		"value":         "hello",
	})
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, tableSchema))

	require.NoError(t, yc.ReshardTable(s.Ctx, testTable, &yt.ReshardTableOptions{
		TabletCount: ptr.Int(6),
	}))

	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	rows := []interface{}{map[string]interface{}{"$tablet_index": 2, "value": "hello"}}
	require.NoError(t, yc.InsertRows(s.Ctx, testTable, rows, nil))

	r, err := yc.SelectRows(s.Ctx, fmt.Sprintf("* from [%s] where [$tablet_index] = 2", testTable), nil)
	require.NoError(t, err)

	var row map[string]interface{}
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	assert.Equal(t, map[string]interface{}{"$tablet_index": int64(2), "$row_index": int64(0), "value": "hello"}, row)
	require.False(t, r.Next())
	require.NoError(t, r.Err())
}
