// package yttest contains testing helpers.
package yttest

import (
	"context"
	"reflect"
	"testing"

	"a.yandex-team.ru/yt/go/schema"

	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"go.uber.org/zap/zaptest"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/yt"
)

type Env struct {
	Ctx context.Context
	YT  yt.Client
	MR  mapreduce.Client
}

func NewEnv(t *testing.T) (env *Env, cancel func()) {
	var err error

	config, err := yt.ClusterFromEnv()
	if err != nil {
		t.Fatalf("failed to get cluster from env: %+v", err)
	}
	config.Logger = &zap.Logger{zaptest.NewLogger(t)}

	env = &Env{}
	env.Ctx, cancel = context.WithCancel(context.Background())
	env.YT, err = ythttp.NewClient(config)
	if err != nil {
		t.Fatalf("failed to create YT client: %+v", err)
	}

	env.MR = mapreduce.New(env.YT)
	return
}

func (e *Env) TmpPath() ypath.Path {
	uid := guid.New()
	return ypath.Path("//tmp").AppendName(uid.String())
}

func (e *Env) UploadSlice(path ypath.Path, slice interface{}) error {
	_, err := e.YT.CreateNode(e.Ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"schema": schema.MustInfer(reflect.New(reflect.ValueOf(slice).Type().Elem()).Interface()),
		},
	})
	if err != nil {
		return err
	}

	w, err := e.YT.WriteTable(e.Ctx, path, nil)
	if err != nil {
		return err
	}

	sliceValue := reflect.ValueOf(slice)
	for i := 0; i < sliceValue.Len(); i++ {
		if err = w.Write(sliceValue.Index(i).Interface()); err != nil {
			return err
		}
	}

	return w.Close()
}

func (e *Env) DownloadSlice(path ypath.Path, value interface{}) error {
	sliceValue := reflect.ValueOf(value).Elem()

	r, err := e.YT.ReadTable(e.Ctx, path, nil)
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

	reflect.ValueOf(value).Elem().Set(sliceValue)
	return nil
}
