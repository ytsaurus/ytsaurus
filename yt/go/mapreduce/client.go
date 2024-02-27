package mapreduce

import (
	"context"
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
	return backoff.Retry(
		func() error {
			return mr.doUploadSelf(ctx)
		},
		backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
}

func (mr *client) doUploadSelf(ctx context.Context) error {
	// TODO(prime@): this is broken with respect to context cancellation

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

	id := guid.New().String()
	tmpPath := ypath.Path("//tmp/go_binary").Child(id[:2]).Child(id)

	_, err = mr.yc.CreateNode(ctx, tmpPath, yt.NodeFile, &yt.CreateNodeOptions{Recursive: true})
	if err != nil {
		return err
	}

	w, err := mr.yc.WriteFile(ctx, tmpPath, nil)
	if err != nil {
		return err
	}

	if _, err = io.Copy(w, exe); err != nil {
		return err
	}

	if err = w.Close(); err != nil {
		return err
	}

	mr.binaryPath = tmpPath
	return nil
}

func (mr *client) operationStartClient() yt.OperationStartClient {
	if mr.tx != nil {
		return mr.tx
	}

	return mr.yc
}
