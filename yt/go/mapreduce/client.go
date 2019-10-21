package mapreduce

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"

	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/yt"
)

type Client interface {
	Map(mapper Job, spec *spec.Spec) (Operation, error)

	Reduce(reducer Job, spec *spec.Spec) (Operation, error)

	JoinReduce(reducer Job, spec *spec.Spec) (Operation, error)

	MapReduce(mapper Job, reducer Job, spec *spec.Spec) (Operation, error)

	MapCombineReduce(mapper Job, combiner Job, reducer Job, spec *spec.Spec) (Operation, error)

	Sort(spec *spec.Spec) (Operation, error)

	Merge(spec *spec.Spec) (Operation, error)

	Erase(spec *spec.Spec) (Operation, error)

	RemoteCopy(spec *spec.Spec) (Operation, error)

	Vanilla(spec *spec.Spec, jobs map[string]Job) (Operation, error)

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
