package ytqueue

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type fakeQueueClient struct {
	yt.Client

	pushStarted chan struct{}
	releasePush chan struct{}
	pushDelay   time.Duration

	mu               sync.Mutex
	pushes           [][]int64
	pushesAtRemoval  int
	sessionRemoved   bool
	sessionsCreated  int
	pushesInProgress int
	maxParallelPush  int
}

func (c *fakeQueueClient) CreateQueueProducerSession(
	ctx context.Context,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	options *yt.CreateQueueProducerSessionOptions,
) (*yt.CreateQueueProducerSessionResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionsCreated++
	return &yt.CreateQueueProducerSessionResult{}, nil
}

func (c *fakeQueueClient) PushQueueProducer(
	ctx context.Context,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	epoch int64,
	rows []any,
	options *yt.PushQueueProducerOptions,
) (*yt.PushQueueProducerResult, error) {
	c.mu.Lock()
	c.pushesInProgress++
	c.maxParallelPush = max(c.maxParallelPush, c.pushesInProgress)
	c.mu.Unlock()

	if c.pushStarted != nil {
		c.pushStarted <- struct{}{}
	}
	if c.releasePush != nil {
		<-c.releasePush
	}
	time.Sleep(c.pushDelay)

	seqNos := make([]int64, 0, len(rows))
	for _, row := range rows {
		seqNos = append(seqNos, row.(ytRow).SequenceNumber)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.pushesInProgress--
	c.pushes = append(c.pushes, seqNos)
	return &yt.PushQueueProducerResult{}, nil
}

func (c *fakeQueueClient) RemoveQueueProducerSession(
	ctx context.Context,
	producerPath ypath.Path,
	queuePath ypath.Path,
	sessionID string,
	options *yt.RemoveQueueProducerSessionOptions,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionRemoved = true
	c.pushesAtRemoval = len(c.pushes)
	return nil
}

func (c *fakeQueueClient) Stop() {}

func (c *fakeQueueClient) pushedBatches() [][]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]int64(nil), c.pushes...)
}

type sentOffsets struct {
	mu      sync.Mutex
	offsets []int64
}

func (s *sentOffsets) onSent(meta pipelines.RowMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets = append(s.offsets, meta.End.LogicalOffset)
}

func newTestOutput(client *fakeQueueClient, bytesPerRowsBatch int, flushTimeout time.Duration, onSent func(pipelines.RowMeta)) *output {
	o := &output{
		yc:                        client,
		sessionID:                 "test-session",
		logger:                    slog.New(slog.NewTextHandler(io.Discard, nil)),
		bytesPerRowsBatch:         bytesPerRowsBatch,
		maxCompressedRowBytes:     MaxCompressedRowBytes,
		rowsBatchFlushTimeout:     flushTimeout,
		largeUncompressedRowBytes: MaxCompressedRowBytes,
		largeCompressedRowBytes:   MaxCompressedRowBytes,
		onSent:                    onSent,
	}
	o.startAsync()
	return o
}

func addRow(ctx context.Context, o *output, seqNo int64, size int) {
	o.Add(ctx, pipelines.RowMeta{
		Begin: pipelines.FilePosition{LogicalOffset: seqNo},
		End:   pipelines.FilePosition{LogicalOffset: seqNo},
	}, pipelines.Row{Payload: make([]byte, size), SeqNo: seqNo})
}

func TestOutputPushesAllRowsInOrder(t *testing.T) {
	ctx := context.Background()
	client := &fakeQueueClient{pushDelay: 20 * time.Millisecond}
	sent := &sentOffsets{}
	o := newTestOutput(client, 250, 0, sent.onSent)

	for seqNo := int64(1); seqNo <= 10; seqNo++ {
		addRow(ctx, o, seqNo, 100)
	}
	o.Close(ctx)

	require.Equal(t, [][]int64{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10}}, client.pushedBatches())
	require.Equal(t, []int64{3, 6, 9, 10}, sent.offsets)
	require.Equal(t, 1, client.sessionsCreated)
	require.True(t, client.sessionRemoved)
	require.Equal(t, 4, client.pushesAtRemoval)
	require.Equal(t, 1, client.maxParallelPush)
}

func TestOutputPushesEachRowWithoutBatching(t *testing.T) {
	ctx := context.Background()
	client := &fakeQueueClient{}
	o := newTestOutput(client, 0, 0, nil)

	for seqNo := int64(1); seqNo <= 5; seqNo++ {
		addRow(ctx, o, seqNo, 100)
	}
	o.Close(ctx)

	require.Equal(t, [][]int64{{1}, {2}, {3}, {4}, {5}}, client.pushedBatches())
}

func TestOutputAccumulatesOneBatchAheadDuringPush(t *testing.T) {
	const pipelineSlackRows = 3

	for _, tc := range []struct {
		name              string
		bytesPerRowsBatch int
		rowSize           int
		rowsPerBatch      int
	}{
		{name: "several_rows_per_batch", bytesPerRowsBatch: 250, rowSize: 100, rowsPerBatch: 3},
		{name: "one_row_per_batch", bytesPerRowsBatch: 250, rowSize: 300, rowsPerBatch: 1},
		{name: "no_batching", bytesPerRowsBatch: 0, rowSize: 100, rowsPerBatch: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client := &fakeQueueClient{
				pushStarted: make(chan struct{}, 100),
				releasePush: make(chan struct{}),
			}
			o := newTestOutput(client, tc.bytesPerRowsBatch, 0, nil)

			const totalRows = 30
			var accepted atomic.Int64
			addDone := make(chan struct{})
			go func() {
				defer close(addDone)
				for seqNo := int64(1); seqNo <= totalRows; seqNo++ {
					addRow(ctx, o, seqNo, tc.rowSize)
					accepted.Add(1)
				}
			}()

			<-client.pushStarted
			want := int64(2*tc.rowsPerBatch + pipelineSlackRows)
			require.Eventually(t, func() bool { return accepted.Load() == want }, 5*time.Second, 10*time.Millisecond)
			require.Never(t, func() bool { return accepted.Load() > want }, 100*time.Millisecond, 10*time.Millisecond)

			go func() {
				for range client.pushStarted {
				}
			}()
			close(client.releasePush)
			<-addDone
			o.Close(ctx)
			close(client.pushStarted)

			var pushedRows int
			for _, batch := range client.pushedBatches() {
				pushedRows += len(batch)
			}
			require.Equal(t, totalRows, pushedRows)
			require.Equal(t, 1, client.maxParallelPush)
		})
	}
}

func TestOutputFlushesPartialBatchByTimeout(t *testing.T) {
	ctx := context.Background()
	client := &fakeQueueClient{}
	o := newTestOutput(client, 1<<20, 200*time.Millisecond, nil)

	addRow(ctx, o, 1, 100)
	addRow(ctx, o, 2, 100)

	require.Eventually(t, func() bool {
		return len(client.pushedBatches()) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, [][]int64{{1, 2}}, client.pushedBatches())

	o.Close(ctx)
	require.Equal(t, [][]int64{{1, 2}}, client.pushedBatches())
}
