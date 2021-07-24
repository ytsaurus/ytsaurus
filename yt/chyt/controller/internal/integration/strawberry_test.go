package integration

import (
	"a.yandex-team.ru/library/go/core/log"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/chyt/controller/internal/sleep"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

var root = ypath.Path("//tmp/strawberry")

func abortAll(t *testing.T, env *yttest.Env) {
	// TODO(max42): introduce some unique annotation and abort only such operations. This would allow
	// running this testsuite on real cluster.
	ops, err := yt.ListAllOperations(env.Ctx, env.YT, &yt.ListOperationsOptions{State: &yt.StateRunning})
	require.NoError(t, err)
	for _, op := range ops {
		err := env.YT.AbortOperation(env.Ctx, op.ID, &yt.AbortOperationOptions{})
		require.NoError(t, err)
	}
}

func prepare(t *testing.T) (*yttest.Env, *strawberry.Agent) {
	env, cancel := yttest.NewEnv(t)
	t.Cleanup(cancel)

	_, err := env.YT.CreateNode(env.Ctx, root, yt.NodeMap, &yt.CreateNodeOptions{Force: true, Recursive: true})
	require.NoError(t, err)

	l := env.L.Logger()

	config := &strawberry.Config{
		Root:                  root,
		PassPeriod:            yson.Duration(time.Millisecond * 500),
		RevisionCollectPeriod: yson.Duration(time.Millisecond * 100),
	}

	abortAll(t, env)

	agent := strawberry.NewAgent("test", env.YT, l, map[string]strawberry.Controller{
		"sleep": sleep.NewController(l.WithName("strawberry"), env.YT, root, "test", nil),
	}, config)

	return env, agent
}

func createNode(t *testing.T, env *yttest.Env, alias string) {
	env.L.Debug("creating node", log.String("alias", alias))
	_, err := env.YT.CreateNode(env.Ctx, root.Child(alias), yt.NodeMap, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"strawberry_family":  "sleep",
			"strawberry_speclet": map[string]interface{}{},
		},
	})
	require.NoError(t, err)
}

func removeNode(t *testing.T, env *yttest.Env, alias string) {
	env.L.Debug("removing node", log.String("alias", alias))
	err := env.YT.RemoveNode(env.Ctx, root.Child(alias), nil)
	require.NoError(t, err)
}

func setAttr(t *testing.T, env *yttest.Env, alias string, attr string, value interface{}) {
	env.L.Debug("setting attribute", log.String("attr", alias+"/@"+attr))
	err := env.YT.SetNode(env.Ctx, root.Child(alias).Attr(attr), value, nil)
	require.NoError(t, err)
}

func getOp(t *testing.T, env *yttest.Env, alias string) *yt.OperationStatus {
	ops, err := yt.ListAllOperations(env.Ctx, env.YT, nil)
	require.NoError(t, err)
	for _, op := range ops {
		if opAlias, ok := op.BriefSpec["alias"].(string); ok && opAlias == "*"+alias {
			return &op
		}
	}
	return nil
}

func waitOp(t *testing.T, env *yttest.Env, alias string) *yt.OperationStatus {
	for i := 0; i < 30; i++ {
		op := getOp(t, env, alias)
		if op != nil {
			return op
		}
		time.Sleep(time.Millisecond * 300)
	}
	t.FailNow()
	return nil
}

func listAliases(t *testing.T, env *yttest.Env) []string {
	ops, err := yt.ListAllOperations(env.Ctx, env.YT, &yt.ListOperationsOptions{State: &yt.StateRunning})
	require.NoError(t, err)

	aliases := make([]string, 0)

	for _, op := range ops {
		if alias, ok := op.BriefSpec["alias"]; ok {
			aliases = append(aliases, alias.(string)[1:])
		}
	}

	return aliases
}

func waitAliases(t *testing.T, env *yttest.Env, expected []string) {
	sort.Strings(expected)

	for i := 0; i < 30; i++ {
		actual := listAliases(t, env)
		sort.Strings(actual)

		if reflect.DeepEqual(expected, actual) {
			return
		}

		time.Sleep(time.Millisecond * 300)
	}
	t.FailNow()
}

func waitIncarnation(t *testing.T, env *yttest.Env, alias string, expected int64) {
	env.L.Debug("waiting for alias incarnation", log.String("alias", alias), log.Int64("expected_incarnation", expected))
	for i := 0; i < 30; i++ {
		op := getOp(t, env, alias)
		if op != nil {
			annotations := op.RuntimeParameters.Annotations
			incarnation, ok := annotations["strawberry_incarnation"]
			require.True(t, ok)
			require.LessOrEqual(t, incarnation, expected)
			if reflect.DeepEqual(incarnation, expected) {
				env.L.Debug("alias reached expected incarnation", log.String("alias", alias), log.Int64("incarnation", expected))
				return
			}
		}
		time.Sleep(time.Millisecond * 300)
	}
	t.FailNow()
}

func TestOperationBeforeStart(t *testing.T) {
	env, agent := prepare(t)
	t.Cleanup(agent.Stop)

	createNode(t, env, "test1")
	agent.Start()

	_ = waitOp(t, env, "test1")
}

func TestOperationAfterStart(t *testing.T) {
	env, agent := prepare(t)
	t.Cleanup(agent.Stop)

	agent.Start()
	time.Sleep(time.Millisecond * 500)

	createNode(t, env, "test2")

	_ = waitOp(t, env, "test2")
}

func TestAbortDangling(t *testing.T) {
	env, agent := prepare(t)
	t.Cleanup(agent.Stop)

	agent.Start()
	createNode(t, env, "test3")
	waitAliases(t, env, []string{"test3"})
	createNode(t, env, "test4")
	waitAliases(t, env, []string{"test3", "test4"})
	removeNode(t, env, "test3")
	waitAliases(t, env, []string{"test4"})
	removeNode(t, env, "test4")
	waitAliases(t, env, []string{})
}

func TestReincarnations(t *testing.T) {
	env, agent := prepare(t)
	t.Cleanup(agent.Stop)

	agent.Start()
	createNode(t, env, "test5")
	waitIncarnation(t, env, "test5", 1)
	setAttr(t, env, "test5", "strawberry_pool", "foo")
	waitIncarnation(t, env, "test5", 2)
	setAttr(t, env, "test5", "strawberry_pool", "bar")
	waitIncarnation(t, env, "test5", 3)
	agent.Stop()
	waitIncarnation(t, env, "test5", 3)
	agent.Start()
	time.Sleep(time.Second * 2)
	waitIncarnation(t, env, "test5", 3)
	setAttr(t, env, "test5", "strawberry_pool", "baz")
	waitIncarnation(t, env, "test5", 4)
}
