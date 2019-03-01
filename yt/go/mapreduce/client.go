package mapreduce

import (
	"context"
	"io"
	"os"
	"sync"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"

	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/yt"
)

type client struct {
	m sync.Mutex

	c yt.Client

	binaryPath ypath.Path
}

func (c *client) uploadSelf(ctx context.Context) error {
	// TODO(prime@): this is broken with respect to context cancellation

	c.m.Lock()
	defer c.m.Unlock()

	if c.binaryPath != "" {
		return nil
	}

	exe, err := os.Open("/proc/self/exe")
	if err != nil {
		return err
	}
	defer func() { _ = exe.Close() }()

	tmpPath := ypath.Path("//tmp").Child(guid.New().String())

	_, err = c.c.CreateNode(ctx, tmpPath, yt.NodeFile, nil)
	if err != nil {
		return err
	}

	w, err := c.c.WriteFile(ctx, tmpPath, nil)
	if err != nil {
		return err
	}

	if _, err = io.Copy(w, exe); err != nil {
		return err
	}

	if err = w.Close(); err != nil {
		return err
	}

	c.binaryPath = tmpPath
	return nil
}

func (c *client) Run(ctx context.Context, spec *spec.Spec) (Operation, error) {
	if err := c.uploadSelf(ctx); err != nil {
		return nil, err
	}

	spec = spec.Clone()
	spec.PatchUserBinary(c.binaryPath)

	id, err := c.c.StartOperation(ctx, spec.Type, spec, nil)
	if err != nil {
		return nil, err
	}

	return &operation{c: c.c, ctx: ctx, opID: id}, nil
}
