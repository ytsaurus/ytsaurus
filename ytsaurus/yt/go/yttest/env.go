// Package yttest contains testing helpers.
package yttest

import (
	"context"
	"reflect"
	"testing"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Env struct {
	Ctx context.Context
	YT  yt.Client
	MR  mapreduce.Client
	L   log.Structured
}

func New(t testing.TB, opts ...Option) *Env {
	env, cancel := NewEnv(t, opts...)
	t.Cleanup(cancel)
	return env
}

func NewEnv(t testing.TB, opts ...Option) (env *Env, cancel func()) {
	var config yt.Config

	var stopLogger func()
	config.Logger, stopLogger = NewLogger(t)

	for i, o := range opts {
		switch v := o.(type) {
		case configOption:
			if i != 0 {
				t.Fatalf("yttest.WithConfig must be the first option")
			}

			config = v.c
		case loggerOption:
			config.Logger = v.l
		}
	}

	var cancelCtx func()
	var err error

	env = &Env{}
	env.Ctx, cancelCtx = context.WithCancel(context.Background())
	env.YT, err = ythttp.NewTestClient(t, &config)
	if err != nil {
		t.Fatalf("failed to create YT client: %+v", err)
	}

	env.MR = mapreduce.New(env.YT)
	env.L = config.Logger

	cancel = func() {
		cancelCtx()
		env.YT.Stop()
		stopLogger()
	}
	return
}

func (e *Env) TmpPath() ypath.Path {
	uid := guid.New()
	return ypath.Path("//tmp").Child(uid.String())
}

func UploadSlice(ctx context.Context, c yt.Client, path ypath.YPath, slice any) error {
	sliceType := reflect.TypeOf(slice)
	if sliceType.Kind() != reflect.Slice {
		return xerrors.Errorf("type %T is not a slice", slice)
	}

	tableSchema, err := schema.Infer(reflect.New(sliceType.Elem()).Interface())
	if err != nil {
		return err
	}

	_, err = c.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{"schema": tableSchema},
	})
	if err != nil && !yterrors.ContainsAlreadyExistsError(err) {
		return err
	}

	w, err := c.WriteTable(ctx, path, nil)
	if err != nil {
		return err
	}

	sliceValue := reflect.ValueOf(slice)
	for i := 0; i < sliceValue.Len(); i++ {
		if err = w.Write(sliceValue.Index(i).Interface()); err != nil {
			return err
		}
	}

	return w.Commit()
}

func (e *Env) UploadSlice(path ypath.YPath, slice any) error {
	return UploadSlice(e.Ctx, e.YT, path, slice)
}

func DownloadSlice(ctx context.Context, c yt.TableClient, path ypath.YPath, value any) error {
	sliceValue := reflect.ValueOf(value).Elem()

	r, err := c.ReadTable(ctx, path, nil)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()

	for r.Next() {
		row := reflect.New(sliceValue.Type().Elem())

		if err = r.Scan(row.Interface()); err != nil {
			return err
		}

		sliceValue = reflect.Append(sliceValue, row.Elem())
	}

	if r.Err() != nil {
		return r.Err()
	}

	reflect.ValueOf(value).Elem().Set(sliceValue)
	return nil
}

func (e *Env) DownloadSlice(path ypath.YPath, value any) error {
	return DownloadSlice(e.Ctx, e.YT, path, value)
}
