package mapreduce

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/tink/go/aead"
	"github.com/google/tink/go/keyset"
	"github.com/google/tink/go/tink"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type Client interface {
	Map(mapper Job, spec *spec.Spec, opts ...OperationOption) (Operation, error)

	RawMap(mapper RawJob, spec *spec.Spec, opts ...OperationOption) (Operation, error)

	Reduce(reducer Job, spec *spec.Spec, opts ...OperationOption) (Operation, error)

	JoinReduce(reducer Job, spec *spec.Spec, opts ...OperationOption) (Operation, error)

	MapReduce(mapper Job, reducer Job, spec *spec.Spec, opts ...OperationOption) (Operation, error)

	MapCombineReduce(mapper Job, combiner Job, reducer Job, spec *spec.Spec, opts ...OperationOption) (Operation, error)

	Sort(spec *spec.Spec, opts ...OperationOption) (Operation, error)

	Merge(spec *spec.Spec, opts ...OperationOption) (Operation, error)

	Erase(spec *spec.Spec, opts ...OperationOption) (Operation, error)

	RemoteCopy(spec *spec.Spec, opts ...OperationOption) (Operation, error)

	// TODO(prime@): switch order of spec and jobs.
	Vanilla(spec *spec.Spec, jobs map[string]Job, opts ...OperationOption) (Operation, error)

	Track(opID yt.OperationID) (Operation, error)

	// WithTx returns Client, that starts all operations inside transaction tx.
	WithTx(tx yt.Tx) Client
}

func New(yc yt.Client, options ...Option) Client {
	mr := &client{
		yc:     yc,
		ctx:    context.Background(),
		config: DefaultConfig(),
	}

	for _, option := range options {
		switch o := option.(type) {
		case *contextOption:
			mr.ctx = o.ctx
		case *configOption:
			mr.config = o.config
		case *defaultACLOption:
			mr.defaultACL = o.acl
		default:
			panic(fmt.Sprintf("received unsupported option of type %T", o))
		}
	}

	var err error
	mr.jobStateKey, err = keyset.NewHandle(aead.AES128CTRHMACSHA256KeyTemplate())
	if err != nil {
		panic(err)
	}
	mr.aead, err = aead.New(mr.jobStateKey)
	if err != nil {
		panic(err)
	}

	return mr
}

type client struct {
	m sync.Mutex

	yc         yt.Client
	tx         yt.Tx
	ctx        context.Context
	config     *Config
	defaultACL []yt.ACE

	jobStateKey *keyset.Handle
	aead        tink.AEAD

	binaryPath ypath.Path
}

func (mr *client) uploadSelf(ctx context.Context) error {
	var b backoff.BackOff = backoff.NewExponentialBackOff()
	if mr.config.UploadSelfBackoff != nil {
		b = mr.config.UploadSelfBackoff
	}
	return backoff.Retry(
		func() error { return mr.doUploadSelf(ctx) },
		backoff.WithContext(b, ctx),
	)
}

func (mr *client) doUploadSelf(ctx context.Context) error {
	mr.m.Lock()
	defer mr.m.Unlock()

	if mr.binaryPath != "" {
		return nil
	}

	exePath, err := os.Executable()
	if err != nil {
		return err
	}

	exe, err := os.Open(exePath)
	if err != nil {
		return err
	}
	defer func() { _ = exe.Close() }()

	tmpDirPath := mr.config.TmpDirPath
	if tmpDirPath == "" {
		tmpDirPath = defaultTmpDir
	}

	cacheDirPath := mr.config.CacheDirPath
	if cacheDirPath == "" {
		cacheDirPath = defaultCacheDir
	}

	cachedPath, err := mr.checkFileInCacheAndUpload(ctx, exe, tmpDirPath, cacheDirPath)
	if err != nil {
		return err
	}
	mr.binaryPath = cachedPath.YPath()

	return nil
}

func (mr *client) checkFileInCacheAndUpload(
	ctx context.Context,
	file *os.File,
	tmpDirPath ypath.Path,
	cacheDirPath ypath.Path,
) (ypath.YPath, error) {
	_, err := mr.yc.CreateNode(ctx, cacheDirPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true, IgnoreExisting: true})
	if err != nil {
		return nil, err
	}

	md5, err := calculateMD5(file)
	if err != nil {
		return nil, err
	}
	path, err := mr.yc.GetFileFromCache(ctx, md5, &yt.GetFileFromCacheOptions{CachePath: cacheDirPath})
	if err != nil {
		return nil, err
	}
	if path != nil {
		return path, nil
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	tmpPath, err := mr.writeFileToTmpDir(ctx, tmpDirPath, file, true)
	if err != nil {
		return nil, err
	}

	cachedPath, err := mr.yc.PutFileToCache(ctx, tmpPath, md5, &yt.PutFileToCacheOptions{CachePath: cacheDirPath})
	if err != nil {
		return nil, err
	}
	if err := mr.yc.RemoveNode(ctx, tmpPath, &yt.RemoveNodeOptions{Force: true}); err != nil {
		return nil, err
	}
	return cachedPath, nil
}

func (mr *client) writeFileToTmpDir(
	ctx context.Context,
	tmpDirPath ypath.Path,
	r io.Reader,
	computeMD5 bool,
) (tmpPath ypath.Path, err error) {
	id := guid.New().String()
	tmpPath = tmpDirPath.Child(id[:2]).Child(id)

	_, err = mr.yc.CreateNode(ctx, tmpPath, yt.NodeFile, &yt.CreateNodeOptions{Recursive: true})
	if err != nil {
		return
	}

	w, err := mr.yc.WriteFile(ctx, tmpPath, &yt.WriteFileOptions{ComputeMD5: computeMD5})
	if err != nil {
		return
	}

	writeErrCh := make(chan error, 1)
	go func() {
		_, err = io.Copy(w, r)
		writeErrCh <- err
	}()

	select {
	case <-ctx.Done():
	case err = <-writeErrCh:
		if err != nil {
			return
		}
	}

	err = w.Close()
	return
}

func (mr *client) operationStartClient() yt.OperationStartClient {
	if mr.tx != nil {
		return mr.tx
	}

	return mr.yc
}

func calculateMD5(r io.Reader) (string, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
