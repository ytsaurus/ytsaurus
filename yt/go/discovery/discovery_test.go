package discovery

import (
	"context"
	"testing"
	"time"

	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/yttest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type MemberMeta struct {
	Version string
	Shard   int
}

func TestSingleJoin(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	g := NewGroup(env.YT, &zap.Logger{zaptest.NewLogger(t)}, Options{Root: env.TmpPath()})

	ctx, cancel := context.WithTimeout(env.Ctx, time.Second*30)
	defer cancel()

	m := MemberMeta{Version: "1.1", Shard: 10}

	leave, err := g.Join(ctx, "first", &m)
	require.NoError(t, err)
	defer leave()

	go g.Update(ctx)

	members := map[string]MemberMeta{}
	require.NoError(t, g.List(ctx, &members))

	require.Equal(t, map[string]MemberMeta{"first": m}, members)
}
