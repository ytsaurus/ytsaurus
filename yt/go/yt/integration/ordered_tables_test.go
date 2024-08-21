package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestOrderedTables(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "OrderedDynamicTable_struct", Test: suite.TestOrderedDynamicTable_struct},
		{Name: "OrderedDynamicTable_map", Test: suite.TestOrderedDynamicTable_map, SkipRPC: true}, // todo https://st.yandex-team.ru/YT-15505
		{Name: "PushQueueProducer_struct", Test: suite.TestPushQueueProducer_struct},
	})
}

type testOrderedTableRow struct {
	TabletIndex int    `yson:"$tablet_index"`
	RowIndex    int    `yson:"$row_index,omitempty"`
	Value       string `yson:"value"`
}

func (s *Suite) TestOrderedDynamicTable_struct(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	tableSchema := schema.MustInfer(&testOrderedTableRow{})
	require.NoError(t, migrate.Create(ctx, yc, testTable, tableSchema))

	require.NoError(t, yc.ReshardTable(ctx, testTable, &yt.ReshardTableOptions{
		TabletCount: ptr.Int(6),
	}))

	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	rows := []any{&testOrderedTableRow{TabletIndex: 2, Value: "hello"}}
	require.NoError(t, yc.InsertRows(ctx, testTable, rows, nil))

	r, err := yc.SelectRows(ctx, fmt.Sprintf("* from [%s] where [$tablet_index] = 2", testTable), nil)
	require.NoError(t, err)

	var row testOrderedTableRow
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	assert.Equal(t, rows[0], &row)
	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func (s *Suite) TestOrderedDynamicTable_map(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	tableSchema := schema.MustInferMap(map[string]any{
		"$tablet_index": 1, // has not effect
		"$row_index":    1, // has not effect
		"value":         "hello",
	})
	require.NoError(t, migrate.Create(ctx, yc, testTable, tableSchema))

	require.NoError(t, yc.ReshardTable(ctx, testTable, &yt.ReshardTableOptions{
		TabletCount: ptr.Int(6),
	}))

	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	rows := []any{map[string]any{"$tablet_index": 2, "value": "hello"}}
	require.NoError(t, yc.InsertRows(ctx, testTable, rows, nil))

	r, err := yc.SelectRows(ctx, fmt.Sprintf("* from [%s] where [$tablet_index] = 2", testTable), nil)
	require.NoError(t, err)

	var row map[string]any
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	assert.Equal(t, map[string]any{"$tablet_index": int64(2), "$row_index": int64(0), "value": "hello"}, row)
	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

type UserMeta struct {
	Value string `yson:"value"`
}

type producerSessionRow struct {
	QueuePath      ypath.Path    `yson:"queue_path"`
	SessionID      string        `yson:"session_id"`
	SequenceNumber int64         `yson:"sequence_number"`
	Epoch          int64         `yson:"epoch"`
	UserMeta       yson.RawValue `yson:"user_meta"`
}

func checkProducerSession(
	ctx context.Context,
	t *testing.T,
	yc yt.Client,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	sequenceNumber int64,
	epoch int64,
	userMeta yson.RawValue,
) {
	expectedProducerSession := producerSessionRow{
		QueuePath:      queuePath,
		SessionID:      sessionID,
		SequenceNumber: sequenceNumber,
		Epoch:          epoch,
		UserMeta:       userMeta,
	}

	r, err := yc.SelectRows(ctx, fmt.Sprintf("* from [%s] where [session_id] = '%s'", producerPath, sessionID), nil)
	require.NoError(t, err)

	var actualProducerSession producerSessionRow
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&actualProducerSession))
	assert.Equal(t, &expectedProducerSession, &actualProducerSession)
	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func pushBatch(
	ctx context.Context,
	yc yt.Client,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	epoch int64,
	startSequenceNumber int64,
) (*yt.PushQueueProducerResult, error) {
	rows := []any{
		&testOrderedTableRow{TabletIndex: 2, Value: "hello"},
		&testOrderedTableRow{TabletIndex: 2, Value: "hello"},
		&testOrderedTableRow{TabletIndex: 2, Value: "hello"},
	}

	return yc.PushQueueProducer(ctx, producerPath, queuePath, sessionID, epoch, rows,
		&yt.PushQueueProducerOptions{SequenceNumber: ptr.Int64(startSequenceNumber)})
}

func checkQueue(
	ctx context.Context,
	t *testing.T,
	yc yt.Client,
	queuePath ypath.Path,
	expectedRowCount int,
) {
	r, err := yc.SelectRows(ctx, fmt.Sprintf("* from [%s] where [$tablet_index] = 2", queuePath), nil)
	require.NoError(t, err)

	for rowIndex := 0; rowIndex < expectedRowCount; rowIndex += 1 {
		expectedRow := &testOrderedTableRow{
			TabletIndex: 2,
			RowIndex:    rowIndex,
			Value:       "hello",
		}

		var actualRow testOrderedTableRow
		require.True(t, r.Next())
		require.NoError(t, r.Scan(&actualRow))
		assert.Equal(t, expectedRow, &actualRow)
	}
	require.False(t, r.Next(), fmt.Sprintf("Only %v rows were expected", expectedRowCount))
	require.NoError(t, r.Err())
}

func (s *Suite) TestPushQueueProducer_struct(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()
	tmpDir := tmpPath()

	queuePath := tmpDir.Child("queue")
	queueSchema := schema.MustInfer(&testOrderedTableRow{})
	require.NoError(t, migrate.Create(ctx, yc, queuePath, queueSchema))

	require.NoError(t, yc.ReshardTable(ctx, queuePath, &yt.ReshardTableOptions{
		TabletCount: ptr.Int(6),
	}))

	require.NoError(t, migrate.MountAndWait(ctx, yc, queuePath))

	producerPath := tmpDir.Child("producer")
	_, err := yc.CreateNode(ctx, producerPath, yt.NodeQueueProducer, &yt.CreateNodeOptions{})
	require.NoError(t, err)

	for i := 0; i < 2; i += 1 {
		sessionID := "session-1"
		options := &yt.CreateQueueProducerSessionOptions{}
		expectedUserMeta, _ := yson.MarshalFormat(nil, yson.FormatBinary)
		if i > 0 {
			options.UserMeta = UserMeta{Value: "123"}
			expectedUserMeta, _ = yson.MarshalFormat(options.UserMeta, yson.FormatBinary)
		}
		createSessionResult, err := yc.CreateQueueProducerSession(
			ctx,
			producerPath,
			queuePath,
			sessionID,
			options,
		)
		require.NoError(t, err)
		require.Equal(t, int64(0), createSessionResult.Epoch)
		require.Equal(t, int64(-1), createSessionResult.SequenceNumber)

		result, err := pushBatch(ctx, yc, producerPath, queuePath, sessionID, 0, 0)

		require.NoError(t, err)
		require.Equal(t, int64(2), result.LastSequenceNumber)
		require.Equal(t, int64(0), result.SkippedRowCount)
		checkQueue(ctx, t, yc, queuePath, 3+(i*10))
		checkProducerSession(ctx, t, yc, producerPath, queuePath, sessionID, 2, 0, expectedUserMeta)

		_, err = pushBatch(ctx, yc, producerPath, queuePath, sessionID, 1, 2)
		require.Error(t, err)

		result, err = pushBatch(ctx, yc, producerPath, queuePath, sessionID, 0, 2)
		require.NoError(t, err)
		require.Equal(t, int64(4), result.LastSequenceNumber)
		require.Equal(t, int64(1), result.SkippedRowCount)
		checkQueue(ctx, t, yc, queuePath, 5+(i*10))
		checkProducerSession(ctx, t, yc, producerPath, queuePath, sessionID, 4, 0, expectedUserMeta)

		_, err = pushBatch(ctx, yc, producerPath, queuePath, sessionID+"-unknown", 0, 3)
		require.Error(t, err)

		result, err = pushBatch(ctx, yc, producerPath, queuePath, sessionID, 0, 4)
		require.NoError(t, err)
		require.Equal(t, int64(6), result.LastSequenceNumber)
		require.Equal(t, int64(1), result.SkippedRowCount)
		checkQueue(ctx, t, yc, queuePath, 7+(i*10))
		checkProducerSession(ctx, t, yc, producerPath, queuePath, sessionID, 6, 0, expectedUserMeta)

		createSessionResult, err = yc.CreateQueueProducerSession(
			ctx,
			producerPath,
			queuePath,
			sessionID,
			&yt.CreateQueueProducerSessionOptions{},
		)
		require.NoError(t, err)
		require.Equal(t, int64(1), createSessionResult.Epoch)
		require.Equal(t, int64(6), createSessionResult.SequenceNumber)

		_, err = pushBatch(ctx, yc, producerPath, queuePath, sessionID, 0, 7)
		require.Error(t, err)

		result, err = pushBatch(ctx, yc, producerPath, queuePath, sessionID, 1, 7)
		require.NoError(t, err)
		require.Equal(t, int64(9), result.LastSequenceNumber)
		require.Equal(t, int64(0), result.SkippedRowCount)
		checkQueue(ctx, t, yc, queuePath, 10+(i*10))
		checkProducerSession(ctx, t, yc, producerPath, queuePath, sessionID, 9, 1, expectedUserMeta)

		require.NoError(t, yc.RemoveQueueProducerSession(
			ctx,
			producerPath,
			queuePath,
			sessionID,
			&yt.RemoveQueueProducerSessionOptions{},
		))
	}
}
