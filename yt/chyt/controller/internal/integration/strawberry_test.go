package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/chyt/controller/internal/sleep"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

var root = ypath.Path("//strawberry")

func prepare(t *testing.T) (yt.Client, *strawberry.Agent) {
	env, cancel := yttest.NewEnv(t)
	t.Cleanup(cancel)

	_, err := env.YT.CreateNode(context.TODO(), root, yt.NodeMap, &yt.CreateNodeOptions{Force: true})
	require.NoError(t, err)

	l := env.L.Logger()

	agent := strawberry.NewAgent("test", env.YT, l, map[string]strawberry.Controller{
		"sleep": sleep.NewController(l, env.YT, root, "test"),
	}, root)

	return env.YT, agent
}

func createNode(t *testing.T, ytc yt.Client, alias string) {
	_, err := ytc.CreateNode(context.TODO(), root.Child(alias), yt.NodeMap, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"strawberry_family":  "sleep",
			"strawberry_speclet": map[string]interface{}{},
		},
	})
	require.NoError(t, err)
}

func getOp(t *testing.T, ytc yt.Client, alias string) *yt.OperationStatus {
	ops, err := yt.ListAllOperations(context.TODO(), ytc, nil)
	require.NoError(t, err)
	for _, op := range ops {
		if opAlias, ok := op.BriefSpec["alias"].(string); ok && opAlias == "*"+alias {
			return &op
		}
	}
	return nil
}

func TestControllerStart(t *testing.T) {
	ytc, agent := prepare(t)

	createNode(t, ytc, "test")
	agent.Start(false, time.Millisecond*100)

	for i := 0; i < 100; i++ {
		op := getOp(t, ytc, "test")
		if op != nil {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
	t.FailNow()
}
