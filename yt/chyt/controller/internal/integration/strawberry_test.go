package integration

import (
	"context"
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

func abortAll(t *testing.T, ytc yt.Client) {
	ops, err := yt.ListAllOperations(context.TODO(), ytc, &yt.ListOperationsOptions{State: yt.StateRunning.Ptr()})
	require.NoError(t, err)
	for _, op := range ops {
		err := ytc.AbortOperation(context.TODO(), op.ID, &yt.AbortOperationOptions{})
		require.NoError(t, err)
	}
}

func prepare(t *testing.T) (yt.Client, *strawberry.Agent) {
	env, cancel := yttest.NewEnv(t)
	t.Cleanup(cancel)

	_, err := env.YT.CreateNode(context.TODO(), root, yt.NodeMap, &yt.CreateNodeOptions{Force: true, Recursive: true})
	require.NoError(t, err)

	l := env.L.Logger()

	config := &strawberry.Config{
		Root:                  root,
		PassPeriod:            yson.Duration(time.Millisecond * 500),
		RevisionCollectPeriod: yson.Duration(time.Millisecond * 100),
	}

	abortAll(t, env.YT)

	agent := strawberry.NewAgent("test", env.YT, l, map[string]strawberry.Controller{
		"sleep": sleep.NewController(l.WithName("strawberry"), env.YT, root, "test", nil),
	}, config)

	return env.YT, agent
}

func createNode(t *testing.T, ytc yt.Client, alias string) {
	t.Logf("creating node %v", alias)
	_, err := ytc.CreateNode(context.TODO(), root.Child(alias), yt.NodeMap, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"strawberry_family":  "sleep",
			"strawberry_speclet": map[string]interface{}{},
		},
	})
	require.NoError(t, err)
}

func removeNode(t *testing.T, ytc yt.Client, alias string) {
	t.Logf("removing node %v", alias)
	err := ytc.RemoveNode(context.TODO(), root.Child(alias), nil)
	require.NoError(t, err)
}

func setAttr(t *testing.T, ytc yt.Client, alias string, attr string, value interface{}) {
	t.Logf("setting attribute %v/@%v", alias, attr)
	err := ytc.SetNode(context.TODO(), root.Child(alias).Attr(attr), value, nil)
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

func waitOp(t *testing.T, ytc yt.Client, alias string) *yt.OperationStatus {
	for i := 0; i < 30; i++ {
		op := getOp(t, ytc, alias)
		if op != nil {
			return op
		}
		time.Sleep(time.Millisecond * 300)
	}
	t.FailNow()
	return nil
}

func listAliases(t *testing.T, ytc yt.Client) []string {
	ops, err := yt.ListAllOperations(context.TODO(), ytc, &yt.ListOperationsOptions{State: yt.StateRunning.Ptr()})
	require.NoError(t, err)

	aliases := make([]string, 0)

	for _, op := range ops {
		if alias, ok := op.BriefSpec["alias"]; ok {
			aliases = append(aliases, alias.(string)[1:])
		}
	}

	return aliases
}

func waitAliases(t *testing.T, ytc yt.Client, expected []string) {
	sort.Strings(expected)

	for i := 0; i < 30; i++ {
		actual := listAliases(t, ytc)
		sort.Strings(actual)

		if reflect.DeepEqual(expected, actual) {
			return
		}

		time.Sleep(time.Millisecond * 300)
	}
	t.FailNow()
}

func waitIncarnation(t *testing.T, ytc yt.Client, alias string, expected int64) {
	t.Logf("waiting for alias %v incarnation %v", alias, expected)
	for i := 0; i < 30; i++ {
		op := getOp(t, ytc, alias)
		if op != nil {
			annotations := op.RuntimeParameters.Annotations
			incarnation, ok := annotations["strawberry_incarnation"]
			require.True(t, ok)
			require.LessOrEqual(t, incarnation, expected)
			if reflect.DeepEqual(incarnation, expected) {
				t.Logf("alias %v is at incarnation %v", alias, expected)
				return
			}
		}
		time.Sleep(time.Millisecond * 300)
	}
	t.FailNow()
}

func TestOperationBeforeStart(t *testing.T) {
	ytc, agent := prepare(t)
	t.Cleanup(agent.Stop)

	createNode(t, ytc, "test1")
	agent.Start()

	_ = waitOp(t, ytc, "test1")
}

func TestOperationAfterStart(t *testing.T) {
	ytc, agent := prepare(t)
	t.Cleanup(agent.Stop)

	agent.Start()
	time.Sleep(time.Millisecond * 500)

	createNode(t, ytc, "test2")

	_ = waitOp(t, ytc, "test2")
}

func TestAbortDangling(t *testing.T) {
	ytc, agent := prepare(t)
	t.Cleanup(agent.Stop)

	agent.Start()
	createNode(t, ytc, "test3")
	waitAliases(t, ytc, []string{"test3"})
	createNode(t, ytc, "test4")
	waitAliases(t, ytc, []string{"test3", "test4"})
	removeNode(t, ytc, "test3")
	waitAliases(t, ytc, []string{"test4"})
	removeNode(t, ytc, "test4")
	waitAliases(t, ytc, []string{})
}

func TestReincarnations(t *testing.T) {
	ytc, agent := prepare(t)
	t.Cleanup(agent.Stop)

	agent.Start()
	createNode(t, ytc, "test5")
	waitIncarnation(t, ytc, "test5", 1)
	setAttr(t, ytc, "test5", "strawberry_pool", "foo")
	waitIncarnation(t, ytc, "test5", 2)
	setAttr(t, ytc, "test5", "strawberry_pool", "bar")
	waitIncarnation(t, ytc, "test5", 3)
	agent.Stop()
	waitIncarnation(t, ytc, "test5", 3)
	agent.Start()
	time.Sleep(time.Second * 2)
	waitIncarnation(t, ytc, "test5", 3)
	setAttr(t, ytc, "test5", "strawberry_pool", "baz")
	waitIncarnation(t, ytc, "test5", 4)
}
