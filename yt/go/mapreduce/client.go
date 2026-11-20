package mapreduce

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/tink"

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
	yc         yt.Client
	tx         yt.Tx
	ctx        context.Context
	config     *Config
	defaultACL []yt.ACE

	jobStateKey *keyset.Handle
	aead        tink.AEAD

	binaryUploadGate contextGate
}

type contextGate struct {
	once  sync.Once
	token chan struct{}
}

func (g *contextGate) acquire(ctx context.Context) error {
	g.once.Do(func() {
		g.token = make(chan struct{}, 1)
		g.token <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.token:
		if err := ctx.Err(); err != nil {
			g.release()
			return err
		}
		return nil
	}
}

func (g *contextGate) release() {
	g.token <- struct{}{}
}

type executable struct {
	file     *os.File
	size     int64
	checksum string
}

func (e *executable) newReader() io.Reader {
	return &localFileReader{file: io.NewSectionReader(e.file, 0, e.size)}
}

type executableCache struct {
	gate   contextGate
	binary *executable
}

var selfBinary executableCache

func (c *executableCache) get(ctx context.Context, exePath string) (*executable, error) {
	if err := c.gate.acquire(ctx); err != nil {
		return nil, err
	}
	defer c.gate.release()
	if c.binary != nil {
		return c.binary, nil
	}
	file, err := os.Open(exePath)
	if err != nil {
		return nil, backoff.Permanent(err)
	}
	hash := md5.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		_ = file.Close()
		return nil, backoff.Permanent(err)
	}
	// Keep the hashed file open for later uploads even if its path is replaced.
	c.binary = &executable{file: file, size: size, checksum: hex.EncodeToString(hash.Sum(nil))}
	return c.binary, nil
}

func (mr *client) uploadSelf(ctx context.Context) (ypath.Path, error) {
	if err := mr.binaryUploadGate.acquire(ctx); err != nil {
		return "", err
	}
	defer mr.binaryUploadGate.release()
	return mr.uploadSelfToCache(ctx)
}

func (mr *client) uploadSelfToCache(ctx context.Context) (ypath.Path, error) {
	exePath := "/proc/self/exe"
	if runtime.GOOS != "linux" {
		var err error
		exePath, err = os.Executable()
		if err != nil {
			return "", backoff.Permanent(err)
		}
	}

	binary, err := selfBinary.get(ctx, exePath)
	if err != nil {
		return "", err
	}

	tmpDirPath := mr.config.TmpDirPath
	if tmpDirPath == "" {
		tmpDirPath = defaultTmpDir
	}

	cacheDirPath := mr.config.CacheDirPath
	if cacheDirPath == "" {
		cacheDirPath = defaultCacheDir
	}

	cachedPath, err := mr.checkFileInCacheAndUpload(ctx, binary, tmpDirPath, cacheDirPath)
	if err != nil {
		return "", err
	}
	return cachedPath.YPath(), nil
}

func (mr *client) checkFileInCacheAndUpload(
	ctx context.Context,
	binary *executable,
	tmpDirPath ypath.Path,
	cacheDirPath ypath.Path,
) (ypath.YPath, error) {
	_, err := mr.yc.CreateNode(ctx, cacheDirPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true, IgnoreExisting: true})
	if err != nil {
		return nil, err
	}

	path, err := mr.yc.GetFileFromCache(ctx, binary.checksum, &yt.GetFileFromCacheOptions{CachePath: cacheDirPath})
	if err != nil {
		return nil, err
	}
	if path != nil {
		return path, nil
	}

	tmpPath, err := mr.writeFileToTmpDir(ctx, tmpDirPath, binary.newReader(), true)
	if err != nil {
		return nil, err
	}

	cachedPath, err := mr.yc.PutFileToCache(ctx, tmpPath, binary.checksum, &yt.PutFileToCacheOptions{CachePath: cacheDirPath})
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
		_, writeErr := io.Copy(w, r)
		writeErrCh <- writeErr
	}()

	select {
	case <-ctx.Done():
		<-writeErrCh
		_ = w.Close()
		return tmpPath, ctx.Err()
	case err = <-writeErrCh:
		if err != nil {
			_ = w.Close()
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
