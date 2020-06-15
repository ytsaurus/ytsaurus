package mapreduce

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cenkalti/backoff/v4"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
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

	return mr
}

type client struct {
	m sync.Mutex

	yc         yt.Client
	tx         yt.Tx
	ctx        context.Context
	config     *Config
	defaultACL []yt.ACE

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

	exe, err := os.Open("/proc/self/exe")
	if err != nil {
		return err
	}
	defer func() { _ = exe.Close() }()

	tmpPath := ypath.Path("//tmp").Child(guid.New().String())

	_, err = mr.yc.CreateNode(ctx, tmpPath, yt.NodeFile, nil)
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
