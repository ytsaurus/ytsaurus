package bench

import (
	"testing"

	"a.yandex-team.ru/yt/go/yson"

	"a.yandex-team.ru/yt/go/yt"

	"a.yandex-team.ru/yt/go/yttest"
)

type singleFieldRow struct {
	A string
}

func BenchmarkWriteTable(b *testing.B) {
	env, cancel := yttest.NewEnv(b)
	defer cancel()

	name := env.TmpPath()
	if _, err := env.YT.CreateNode(env.Ctx, name, yt.NodeTable, nil); err != nil {
		b.Fatal(err)
	}

	w, err := env.YT.WriteTable(env.Ctx, name, nil)
	if err != nil {
		b.Fatal(err)
	}

	row := singleFieldRow{"foobar"}
	for i := 0; i < b.N; i++ {
		if err := w.Write(row); err != nil {
			b.Fatal(err)
		}
	}

	ys, _ := yson.Marshal(row)
	b.SetBytes(int64(len(ys)))

	if err = w.Commit(); err != nil {
		b.Fatal(err)
	}
}
