package mapreduce

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type fileTestClient struct {
	yt.Client

	beginTx          func(context.Context, *yt.StartTxOptions) (yt.Tx, error)
	createNode       func(context.Context, ypath.YPath, yt.NodeType, *yt.CreateNodeOptions) (yt.NodeID, error)
	writeFile        func(context.Context, ypath.YPath, *yt.WriteFileOptions) (io.WriteCloser, error)
	getOperation     func(context.Context, yt.OperationID, *yt.GetOperationOptions) (*yt.OperationStatus, error)
	removeNode       func(context.Context, ypath.YPath, *yt.RemoveNodeOptions) error
	getFileFromCache func(context.Context, string, *yt.GetFileFromCacheOptions) (ypath.YPath, error)
	putFileToCache   func(context.Context, ypath.YPath, string, *yt.PutFileToCacheOptions) (ypath.YPath, error)
	startOperation   func(context.Context, yt.OperationType, any, *yt.StartOperationOptions) (yt.OperationID, error)
}

func (c *fileTestClient) BeginTx(ctx context.Context, opts *yt.StartTxOptions) (yt.Tx, error) {
	return c.beginTx(ctx, opts)
}

func (c *fileTestClient) CreateNode(ctx context.Context, path ypath.YPath, typ yt.NodeType, opts *yt.CreateNodeOptions) (yt.NodeID, error) {
	return c.createNode(ctx, path, typ, opts)
}

func (c *fileTestClient) WriteFile(ctx context.Context, path ypath.YPath, opts *yt.WriteFileOptions) (io.WriteCloser, error) {
	return c.writeFile(ctx, path, opts)
}

func (c *fileTestClient) GetOperation(ctx context.Context, id yt.OperationID, opts *yt.GetOperationOptions) (*yt.OperationStatus, error) {
	return c.getOperation(ctx, id, opts)
}

func (c *fileTestClient) RemoveNode(ctx context.Context, path ypath.YPath, opts *yt.RemoveNodeOptions) error {
	return c.removeNode(ctx, path, opts)
}

func (c *fileTestClient) GetFileFromCache(ctx context.Context, md5 string, opts *yt.GetFileFromCacheOptions) (ypath.YPath, error) {
	return c.getFileFromCache(ctx, md5, opts)
}

func (c *fileTestClient) PutFileToCache(ctx context.Context, path ypath.YPath, md5 string, opts *yt.PutFileToCacheOptions) (ypath.YPath, error) {
	return c.putFileToCache(ctx, path, md5, opts)
}

func (c *fileTestClient) StartOperation(ctx context.Context, typ yt.OperationType, s any, opts *yt.StartOperationOptions) (yt.OperationID, error) {
	return c.startOperation(ctx, typ, s, opts)
}

type fileTestTx struct {
	yt.Tx

	beginTx    func(context.Context, *yt.StartTxOptions) (yt.Tx, error)
	lockNode   func(context.Context, ypath.YPath, yt.LockMode, *yt.LockNodeOptions) (yt.LockResult, error)
	finished   chan struct{}
	finishOnce sync.Once
	aborted    bool
}

func (tx *fileTestTx) BeginTx(ctx context.Context, opts *yt.StartTxOptions) (yt.Tx, error) {
	return tx.beginTx(ctx, opts)
}

func (tx *fileTestTx) LockNode(ctx context.Context, path ypath.YPath, mode yt.LockMode, opts *yt.LockNodeOptions) (yt.LockResult, error) {
	return tx.lockNode(ctx, path, mode, opts)
}

func (tx *fileTestTx) finish() {
	tx.finishOnce.Do(func() { close(tx.finished) })
}

func (tx *fileTestTx) Abort() error {
	tx.aborted = true
	tx.finish()
	return nil
}

func (tx *fileTestTx) Finished() <-chan struct{} {
	return tx.finished
}

func TestBeginFileTransactionRetryPolicy(t *testing.T) {
	beginErr := yterrors.Err(yterrors.CodeTransportError, yterrors.Err(yterrors.CodeNoSuchTransaction))
	attempts := 0
	locks := &operationFileLocks{parentTx: &fileTestTx{
		beginTx: func(context.Context, *yt.StartTxOptions) (yt.Tx, error) {
			attempts++
			return nil, beginErr
		},
	}}
	defer locks.abort()

	err := locks.retry(context.Background(), backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 1), func() error {
		return locks.begin(context.Background())
	})
	require.Equal(t, 1, attempts)
	require.ErrorIs(t, err, beginErr)
	require.Nil(t, locks.tx)
}

func TestBeginFileTransactionCreatedAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := &fileTestTx{finished: make(chan struct{})}
	locks := &operationFileLocks{yc: &fileTestClient{
		beginTx: func(beginCtx context.Context, _ *yt.StartTxOptions) (yt.Tx, error) {
			cancel()
			select {
			case <-beginCtx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("transaction creation ignored cancellation")
			}
			return tx, nil
		},
	}}

	require.ErrorIs(t, locks.begin(ctx), context.Canceled)
	require.Nil(t, locks.tx)
	require.True(t, tx.aborted)
}

func TestUploadJobStateStopsAfterTransactionLoss(t *testing.T) {
	for _, phase := range []string{"during_lock", "server_rejected_lock"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tx := &fileTestTx{finished: make(chan struct{})}
			lockErr := yterrors.Err(yterrors.CodeNoSuchTransaction)
			lockCalls := 0
			tx.lockNode = func(context.Context, ypath.YPath, yt.LockMode, *yt.LockNodeOptions) (yt.LockResult, error) {
				lockCalls++
				if phase == "during_lock" {
					tx.finish()
					return yt.LockResult{}, errors.New("transaction aborted")
				}
				return yt.LockResult{}, lockErr
			}
			creates := 0
			writer := &happyWriter{}
			yc := &fileTestClient{
				createNode: func(context.Context, ypath.YPath, yt.NodeType, *yt.CreateNodeOptions) (yt.NodeID, error) {
					creates++
					return yt.NodeID{}, nil
				},
				writeFile: func(context.Context, ypath.YPath, *yt.WriteFileOptions) (io.WriteCloser, error) {
					return writer, nil
				},
			}
			p := &prepare{mr: &client{yc: yc, config: DefaultConfig(), aead: &plaintextAEAD{}}, fileLocks: operationFileLocks{tx: tx}}
			script := &spec.UserScript{}
			err := p.uploadJobState(script, &jobState{})(ctx, p)
			if phase == "server_rejected_lock" {
				require.ErrorIs(t, err, lockErr)
			} else {
				require.ErrorIs(t, err, errFileTransactionFinished)
			}
			require.Empty(t, script.FilePaths)
			require.Equal(t, 1, creates)
			require.Equal(t, 1, lockCalls)
			require.True(t, writer.closed)
			require.Len(t, p.uploadedFiles, 1)
		})
	}
}

func TestUploadJobStateDoesNotRetryCreateError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "authorization", err: yterrors.Err(yterrors.CodeAuthorizationError)},
		{name: "resolve", err: yterrors.Err(yterrors.CodeResolveError)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			tx := &fileTestTx{finished: make(chan struct{})}
			var created, removed []ypath.Path
			yc := &fileTestClient{
				createNode: func(_ context.Context, path ypath.YPath, _ yt.NodeType, _ *yt.CreateNodeOptions) (yt.NodeID, error) {
					created = append(created, path.YPath())
					return yt.NodeID{}, tc.err
				},
				removeNode: func(_ context.Context, path ypath.YPath, _ *yt.RemoveNodeOptions) error {
					removed = append(removed, path.YPath())
					return nil
				},
			}
			p := &prepare{ctx: ctx, mr: &client{yc: yc, config: DefaultConfig(), aead: &plaintextAEAD{}}, fileLocks: operationFileLocks{tx: tx}}
			defer p.fileLocks.abort()
			script := &spec.UserScript{}

			err := p.uploadJobState(script, &jobState{})(ctx, p)
			require.ErrorIs(t, err, tc.err)
			require.Len(t, created, 1)
			require.Empty(t, script.FilePaths)
			p.removeUploadedFiles()
			require.Equal(t, created, removed)
		})
	}
}

func TestFileLocksRetryReportsTransactionLoss(t *testing.T) {
	tx := &fileTestTx{finished: make(chan struct{})}
	locks := &operationFileLocks{tx: tx}
	defer locks.abort()

	err := locks.retry(context.Background(), backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 1), func() error {
		tx.finish()
		return backoff.Permanent(errors.New("write failed"))
	})
	require.ErrorIs(t, err, errFileTransactionFinished)
}

func TestReleaseFileTransaction(t *testing.T) {
	type response struct {
		state yt.OperationState
		err   error
	}
	retryThenPrepare := func(err error) []response {
		return []response{{err: err}, {state: yt.StatePreparing}}
	}

	for _, tc := range []struct {
		name      string
		responses []response
		finished  bool
	}{
		{
			name: "initialization",
			responses: []response{
				{state: yt.StateStarting},
				{state: yt.StatePending},
				{state: yt.StateInitializing},
				{state: yt.StatePreparing},
			},
		},
		{name: "request_timeout_during_initialization", responses: retryThenPrepare(yterrors.Err("get operation", context.DeadlineExceeded))},
		{name: "permanent_error", responses: []response{{err: yterrors.Err(yterrors.CodeAuthorizationError)}}},
		{name: "transaction_finished", responses: []response{{state: yt.StateInitializing}}, finished: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tx := &fileTestTx{finished: make(chan struct{})}
			timer := time.AfterFunc(30*time.Second, tx.finish)
			defer timer.Stop()
			id := yt.OperationID(guid.New())
			polls := 0
			yc := &fileTestClient{
				getOperation: func(ctx context.Context, operationID yt.OperationID, opts *yt.GetOperationOptions) (*yt.OperationStatus, error) {
					require.NoError(t, ctx.Err())
					require.Equal(t, id, operationID)
					require.Equal(t, []string{"state"}, opts.Attributes)
					require.False(t, tx.aborted)
					require.Less(t, polls, len(tc.responses))
					result := tc.responses[polls]
					polls++
					if tc.finished {
						tx.finish()
					}
					if result.err != nil {
						return nil, result.err
					}
					return &yt.OperationStatus{State: result.state}, nil
				},
			}
			pollInterval := time.Millisecond
			if tc.finished {
				pollInterval = time.Hour
			}
			locks := &operationFileLocks{yc: yc, tx: tx, pollInterval: pollInterval}
			locks.pollOperation(ctx, id)
			require.True(t, tx.aborted)
			require.Equal(t, len(tc.responses), polls)
		})
	}
}

func TestLockPathRetryPolicy(t *testing.T) {
	for _, tc := range []struct {
		name         string
		firstError   error
		wantAttempts int
	}{
		{name: "authorization_in_transport_error", firstError: yterrors.Err(yterrors.CodeTransportError, yterrors.Err(yterrors.CodeAuthorizationError)), wantAttempts: 1},
		{name: "removed_file", firstError: yterrors.Err(yterrors.CodeResolveError), wantAttempts: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fileTestTx{finished: make(chan struct{})}
			attempts := 0
			tx.lockNode = func(context.Context, ypath.YPath, yt.LockMode, *yt.LockNodeOptions) (yt.LockResult, error) {
				attempts++
				if attempts == 1 {
					return yt.LockResult{}, tc.firstError
				}
				return yt.LockResult{NodeID: yt.NodeID(guid.New())}, nil
			}
			locks := &operationFileLocks{tx: tx}
			err := locks.retry(context.Background(), backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 1), func() error {
				_, err := locks.lockPath(context.Background(), "//tmp/job-state")
				return err
			})
			require.Equal(t, tc.wantAttempts, attempts)
			if tc.wantAttempts == 1 {
				require.ErrorIs(t, err, tc.firstError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type uploadTestWriter struct {
	closeError error
}

func (w uploadTestWriter) Write(data []byte) (int, error) { return len(data), nil }

func (w uploadTestWriter) Close() error { return w.closeError }

func TestUploadJobStateRemovesFailedAttempts(t *testing.T) {
	truncated := yterrors.Err("write_file failed", io.ErrUnexpectedEOF)
	for _, tc := range []struct {
		name        string
		createError error
		writer      uploadTestWriter
		lockError   error
	}{
		{name: "lock_error", lockError: yterrors.Err(yterrors.CodeTransportError)},
		{name: "create_response_lost", createError: yterrors.Err(yterrors.CodeTransportError)},
		{name: "close_response_truncated", writer: uploadTestWriter{closeError: truncated}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fileTestTx{finished: make(chan struct{})}
			lockCalls := 0
			tx.lockNode = func(context.Context, ypath.YPath, yt.LockMode, *yt.LockNodeOptions) (yt.LockResult, error) {
				lockCalls++
				if lockCalls == 1 && tc.lockError != nil {
					return yt.LockResult{}, tc.lockError
				}
				return yt.LockResult{NodeID: yt.NodeID(guid.New())}, nil
			}
			var attempts, removed []ypath.Path
			yc := &fileTestClient{
				createNode: func(_ context.Context, path ypath.YPath, _ yt.NodeType, opts *yt.CreateNodeOptions) (yt.NodeID, error) {
					require.NotNil(t, opts)
					require.True(t, opts.Recursive)
					attempts = append(attempts, path.YPath())
					if len(attempts) == 1 {
						return yt.NodeID{}, tc.createError
					}
					return yt.NodeID{}, nil
				},
				writeFile: func(context.Context, ypath.YPath, *yt.WriteFileOptions) (io.WriteCloser, error) {
					if len(attempts) == 1 {
						return tc.writer, nil
					}
					return &happyWriter{}, nil
				},
				removeNode: func(_ context.Context, path ypath.YPath, _ *yt.RemoveNodeOptions) error {
					removed = append(removed, path.YPath())
					return nil
				},
			}
			p := &prepare{mr: &client{yc: yc, config: DefaultConfig(), aead: &plaintextAEAD{}}, fileLocks: operationFileLocks{tx: tx}}
			script := &spec.UserScript{}
			require.NoError(t, p.uploadJobState(script, &jobState{})(context.Background(), p))
			require.Len(t, attempts, 2)
			require.Equal(t, []ypath.Path{attempts[0]}, removed)
			require.Len(t, script.FilePaths, 1)
			require.Equal(t, attempts[1], script.FilePaths[0].OriginalPath)
		})
	}
}

func TestContextUntilFinished(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := &fileTestTx{finished: make(chan struct{})}
	locks := &operationFileLocks{tx: tx}
	actionCtx, cancelAction := locks.contextUntilFinished(ctx)
	defer cancelAction()
	require.NoError(t, actionCtx.Err())

	tx.finish()
	select {
	case <-actionCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("action context was not canceled after the transaction finished")
	}
	require.ErrorIs(t, actionCtx.Err(), context.Canceled)
	require.NoError(t, ctx.Err())
}

type cancelAwareFileTx struct {
	yt.Tx
	ctx      context.Context
	finished chan struct{}
	unblock  chan struct{}
}

func (tx *cancelAwareFileTx) Abort() error {
	select {
	case <-tx.ctx.Done():
	case <-tx.unblock:
	}
	return nil
}

func (tx *cancelAwareFileTx) Finished() <-chan struct{} {
	return tx.finished
}

func TestAbortFileTransactionCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tx := &cancelAwareFileTx{ctx: ctx, finished: make(chan struct{}), unblock: make(chan struct{})}
	defer close(tx.unblock)
	locks := &operationFileLocks{tx: tx, cancel: cancel}
	done := make(chan struct{})
	go func() {
		locks.abort()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("abort did not cancel the transaction context")
	}
}

func TestUploadSelfLocksCachedPathPerOperation(t *testing.T) {
	var transactions []*fileTestTx
	lockCalls := 0
	var checksums []string
	nodeID := yt.NodeID(guid.New())
	yc := &fileTestClient{
		beginTx: func(context.Context, *yt.StartTxOptions) (yt.Tx, error) {
			tx := &fileTestTx{finished: make(chan struct{})}
			tx.lockNode = func(_ context.Context, path ypath.YPath, mode yt.LockMode, _ *yt.LockNodeOptions) (yt.LockResult, error) {
				require.Equal(t, ypath.Path("//cache/binary"), path.YPath())
				require.Equal(t, yt.LockSnapshot, mode)
				lockCalls++
				return yt.LockResult{NodeID: nodeID}, nil
			}
			transactions = append(transactions, tx)
			return tx, nil
		},
		createNode: func(context.Context, ypath.YPath, yt.NodeType, *yt.CreateNodeOptions) (yt.NodeID, error) {
			return yt.NodeID{}, nil
		},
		getFileFromCache: func(_ context.Context, checksum string, _ *yt.GetFileFromCacheOptions) (ypath.YPath, error) {
			checksums = append(checksums, checksum)
			return ypath.Path("//cache/binary"), nil
		},
	}
	mr := New(yc).(*client)
	for i := 0; i < 2; i++ {
		p := mr.prepare(spec.Vanilla())
		file, err := p.uploadSelf(context.Background())
		require.NoError(t, err)
		require.Equal(t, ypath.Path("//cache/binary"), file.OriginalPath)
		require.Equal(t, "go-binary", file.FileName)
		require.True(t, file.Executable)
		require.Equal(t, nodeID.YPath(), file.CypressPath)
		p.fileLocks.abort()
	}
	require.Len(t, transactions, 2)
	require.Equal(t, 2, lockCalls)
	require.Len(t, checksums, 2)
	require.NotEmpty(t, checksums[0])
	require.Equal(t, checksums[0], checksums[1])
	require.NotSame(t, transactions[0], transactions[1])
}

func TestLocalFilesRetryAndCleanup(t *testing.T) {
	localPath := filepath.Join(t.TempDir(), "local")
	require.NoError(t, os.WriteFile(localPath, []byte("contents"), 0700))
	tx := &fileTestTx{finished: make(chan struct{})}
	localLocks := 0
	localID := yt.NodeID(guid.New())
	tx.lockNode = func(_ context.Context, _ ypath.YPath, mode yt.LockMode, _ *yt.LockNodeOptions) (yt.LockResult, error) {
		require.Equal(t, yt.LockSnapshot, mode)
		localLocks++
		if localLocks == 1 {
			return yt.LockResult{}, yterrors.Err(yterrors.CodeResolveError)
		}
		return yt.LockResult{NodeID: localID}, nil
	}
	var created, removed []ypath.Path
	var writers []*happyWriter
	startErr := errors.New("start failed")
	yc := &fileTestClient{
		beginTx: func(context.Context, *yt.StartTxOptions) (yt.Tx, error) { return tx, nil },
		createNode: func(_ context.Context, path ypath.YPath, _ yt.NodeType, _ *yt.CreateNodeOptions) (yt.NodeID, error) {
			created = append(created, path.YPath())
			return yt.NodeID{}, nil
		},
		writeFile: func(context.Context, ypath.YPath, *yt.WriteFileOptions) (io.WriteCloser, error) {
			w := &happyWriter{}
			writers = append(writers, w)
			return w, nil
		},
		startOperation: func(_ context.Context, _ yt.OperationType, raw any, _ *yt.StartOperationOptions) (yt.OperationID, error) {
			require.Len(t, created, 2)
			require.NotEqual(t, created[0], created[1])
			require.Equal(t, []ypath.Path{created[0]}, removed)
			files := raw.(*spec.Spec).Tasks["test"].FilePaths
			require.Len(t, files, 2)
			require.Equal(t, spec.File{CypressPath: "//user/file", FileName: "user"}, files[0])
			require.Equal(t, "local", files[1].FileName)
			require.True(t, files[1].Executable)
			require.Equal(t, created[1], files[1].OriginalPath)
			require.Equal(t, localID.YPath(), files[1].CypressPath)
			return yt.OperationID{}, startErr
		},
		removeNode: func(_ context.Context, path ypath.YPath, opts *yt.RemoveNodeOptions) error {
			require.True(t, opts.Force)
			removed = append(removed, path.YPath())
			return nil
		},
	}
	s := spec.Vanilla().AddVanillaTask("test", 1)
	s.Tasks["test"].FilePaths = []spec.File{{CypressPath: "//user/file", FileName: "user"}}
	p := New(yc).(*client).prepare(s)
	_, err := p.start([]OperationOption{SkipSelfUpload(), WithLocalFiles([]string{localPath})})
	require.ErrorIs(t, err, startErr)
	require.Equal(t, 2, localLocks)
	require.Len(t, writers, 2)
	require.Equal(t, []ypath.Path{created[0], created[0], created[1]}, removed)
	require.NotContains(t, removed, ypath.Path("//user/file"))
	require.True(t, tx.aborted)
	for _, w := range writers {
		require.Equal(t, "contents", string(w.written))
		require.True(t, w.closed)
	}
}

func TestUploadSelfRetriesAfterCachedFileRemoval(t *testing.T) {
	tx := &fileTestTx{finished: make(chan struct{})}
	lockCalls := 0
	lookups := 0
	nodeID := yt.NodeID(guid.New())
	tx.lockNode = func(_ context.Context, path ypath.YPath, _ yt.LockMode, _ *yt.LockNodeOptions) (yt.LockResult, error) {
		lockCalls++
		if lockCalls == 1 {
			return yt.LockResult{}, yterrors.Err(yterrors.CodeResolveError)
		}
		require.Equal(t, ypath.Path("//cache/new"), path.YPath())
		return yt.LockResult{NodeID: nodeID}, nil
	}
	yc := &fileTestClient{
		createNode: func(context.Context, ypath.YPath, yt.NodeType, *yt.CreateNodeOptions) (yt.NodeID, error) {
			return yt.NodeID{}, nil
		},
		getFileFromCache: func(context.Context, string, *yt.GetFileFromCacheOptions) (ypath.YPath, error) {
			lookups++
			if lookups > 1 {
				return nil, nil
			}
			return ypath.Path("//cache/binary"), nil
		},
		writeFile: func(context.Context, ypath.YPath, *yt.WriteFileOptions) (io.WriteCloser, error) {
			return uploadTestWriter{}, nil
		},
		putFileToCache: func(context.Context, ypath.YPath, string, *yt.PutFileToCacheOptions) (ypath.YPath, error) {
			return ypath.Path("//cache/new"), nil
		},
		removeNode: func(context.Context, ypath.YPath, *yt.RemoveNodeOptions) error { return nil },
	}
	config := DefaultConfig()
	config.UploadSelfBackoff = backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 1)
	p := &prepare{mr: &client{yc: yc, config: config}, fileLocks: operationFileLocks{tx: tx}}
	defer p.fileLocks.abort()

	file, err := p.uploadSelf(context.Background())
	require.NoError(t, err)
	require.Equal(t, nodeID.YPath(), file.CypressPath)
	require.Equal(t, 2, lockCalls)
	require.Equal(t, 2, lookups)
}

func TestUploadSelfRetryPolicy(t *testing.T) {
	for _, tc := range []struct {
		name     string
		step     string
		firstErr error
		attempts int
	}{
		{name: "cache_directory_denied", step: "cache_directory", firstErr: yterrors.Err(yterrors.CodeAuthorizationError), attempts: 1},
		{name: "close_response_truncated", step: "close", firstErr: yterrors.Err("write_file failed", io.ErrUnexpectedEOF), attempts: 2},
		{name: "cache_put_conflict", step: "put", firstErr: yterrors.Err(yterrors.CodeConcurrentTransactionLockConflict), attempts: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fileTestTx{finished: make(chan struct{})}
			nodeID := yt.NodeID(guid.New())
			tx.lockNode = func(context.Context, ypath.YPath, yt.LockMode, *yt.LockNodeOptions) (yt.LockResult, error) {
				return yt.LockResult{NodeID: nodeID}, nil
			}
			attempts := 0
			yc := &fileTestClient{
				createNode: func(_ context.Context, _ ypath.YPath, typ yt.NodeType, _ *yt.CreateNodeOptions) (yt.NodeID, error) {
					if typ == yt.NodeMap {
						attempts++
						if attempts == 1 && tc.step == "cache_directory" {
							return yt.NodeID{}, tc.firstErr
						}
					}
					return yt.NodeID{}, nil
				},
				getFileFromCache: func(context.Context, string, *yt.GetFileFromCacheOptions) (ypath.YPath, error) {
					return nil, nil
				},
				writeFile: func(context.Context, ypath.YPath, *yt.WriteFileOptions) (io.WriteCloser, error) {
					if attempts == 1 && tc.step == "close" {
						return uploadTestWriter{closeError: tc.firstErr}, nil
					}
					return uploadTestWriter{}, nil
				},
				putFileToCache: func(context.Context, ypath.YPath, string, *yt.PutFileToCacheOptions) (ypath.YPath, error) {
					if attempts == 1 && tc.step == "put" {
						return nil, tc.firstErr
					}
					return ypath.Path("//cache/binary"), nil
				},
				removeNode: func(context.Context, ypath.YPath, *yt.RemoveNodeOptions) error {
					return nil
				},
			}
			config := DefaultConfig()
			config.UploadSelfBackoff = backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 1)
			p := &prepare{mr: &client{yc: yc, config: config}, fileLocks: operationFileLocks{tx: tx}}
			defer p.fileLocks.abort()

			file, err := p.uploadSelf(context.Background())
			if tc.attempts == 1 {
				require.ErrorIs(t, err, tc.firstErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, nodeID.YPath(), file.CypressPath)
			}
			require.Equal(t, tc.attempts, attempts)
		})
	}
}
