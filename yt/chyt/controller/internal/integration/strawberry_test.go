package integration

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/sleep"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

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

func createAgent(env *yttest.Env, stage string) *strawberry.Agent {
	l := log.With(env.L.Logger(), log.String("agent_stage", stage))

	config := &strawberry.Config{
		Root:                  root,
		PassPeriod:            yson.Duration(time.Millisecond * 500),
		RevisionCollectPeriod: yson.Duration(time.Millisecond * 100),
		Stage:                 stage,
	}

	agent := strawberry.NewAgent("test", env.YT, l, sleep.NewController(l.WithName("strawberry"), env.YT, root, "test", nil), config)

	return agent
}

func prepare(t *testing.T) (*yttest.Env, *strawberry.Agent) {
	env, cancel := yttest.NewEnv(t)
	t.Cleanup(cancel)

	err := env.YT.RemoveNode(env.Ctx, root, &yt.RemoveNodeOptions{Force: true, Recursive: true})
	require.NoError(t, err)
	_, err = env.YT.CreateNode(env.Ctx, root, yt.NodeMap, &yt.CreateNodeOptions{Force: true, Recursive: true})
	require.NoError(t, err)

	abortAll(t, env)

	agent := createAgent(env, "default")

	return env, agent
}

func createStrawberryOp(t *testing.T, env *yttest.Env, alias string) {
	env.L.Debug("creating node", log.String("alias", alias))
	_, err := env.YT.CreateNode(env.Ctx, root.Child(alias), yt.NodeMap, nil)
	require.NoError(t, err)
	_, err = env.YT.CreateNode(env.Ctx, root.Child(alias).Child("access"), yt.NodeMap, nil)
	require.NoError(t, err)
	_, err = env.YT.CreateNode(env.Ctx, root.Child(alias).Child("speclet"), yt.NodeDocument, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"value": map[string]interface{}{
				"active":                    true,
				"family":                    "sleep",
				"stage":                     "default",
				"restart_on_speclet_change": true,
			},
		},
	})
	require.NoError(t, err)
}

func removeNode(t *testing.T, env *yttest.Env, alias string) {
	env.L.Debug("removing node", log.String("alias", alias))
	err := env.YT.RemoveNode(
		env.Ctx,
		root.Child(alias),
		&yt.RemoveNodeOptions{
			Recursive: true,
		})
	require.NoError(t, err)
}

type opletState struct {
	persistentState   strawberry.PersistentState
	infoState         strawberry.InfoState
	stateRevision     yt.Revision
	strawberrySpeclet strawberry.Speclet
	specletRevision   yt.Revision
}

func getOpletState(t *testing.T, env *yttest.Env, alias string) opletState {
	var node struct {
		PersistentState strawberry.PersistentState `yson:"strawberry_persistent_state,attr"`
		InfoState       strawberry.InfoState       `yson:"strawberry_info_state,attr"`
		Revision        yt.Revision                `yson:"revision,attr"`
		Speclet         struct {
			Value    strawberry.Speclet `yson:"value,attr"`
			Revision yt.Revision        `yson:"revision,attr"`
		} `yson:"speclet"`
	}

	// Keep in sync with structure above.
	attributes := []string{
		"strawberry_persistent_state", "strawberry_info_state", "revision", "value",
	}

	err := env.YT.GetNode(env.Ctx, root.Child(alias), &node, &yt.GetNodeOptions{Attributes: attributes})
	require.NoError(t, err)

	return opletState{
		node.PersistentState,
		node.InfoState,
		node.Revision,
		node.Speclet.Value,
		node.Speclet.Revision,
	}
}

func setAttr(t *testing.T, env *yttest.Env, alias string, attr string, value interface{}) {
	env.L.Debug("setting attribute", log.String("attr", alias+"/@"+attr))
	err := env.YT.SetNode(env.Ctx, root.Child(alias).Attr(attr), value, nil)
	require.NoError(t, err)
}

func setSpecletOption(t *testing.T, env *yttest.Env, alias string, option string, value interface{}) {
	env.L.Debug("setting speclet option", log.String("alias", alias), log.String("option", option), log.Any("value", value))
	err := env.YT.SetNode(env.Ctx, root.Child(alias).Child("speclet").Child(option), value, nil)
	require.NoError(t, err)
}

func setACL(t *testing.T, env *yttest.Env, alias string, acl []yt.ACE) {
	err := env.YT.SetNode(env.Ctx, root.Child(alias).Child("access").Attr("acl"), acl, nil)
	require.NoError(t, err)
	// TODO(dakovalkov): Changing ACL does not change the revision now.
	// We set user attribute now to force changing the revision.
	// Remove it after https://st.yandex-team.ru/YT-16169.
	err = env.YT.SetNode(env.Ctx, root.Child(alias).Child("access").Attr("_idm_integration"), true, nil)
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

func getOpStage(t *testing.T, env *yttest.Env, alias string) string {
	op := getOp(t, env, alias)
	require.NotEqual(t, op, nil)
	annotations := op.RuntimeParameters.Annotations
	stage, ok := annotations["strawberry_stage"]
	require.True(t, ok)
	return stage.(string)
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

func getOpACL(t *testing.T, env *yttest.Env, alias string) []yt.ACE {
	op := getOp(t, env, alias)
	if op == nil {
		return nil
	}
	acl := op.RuntimeParameters.ACL
	// Delete unused fields.
	for i := range acl {
		acl[i].InheritanceMode = ""
	}
	return acl
}

func waitOpACL(t *testing.T, env *yttest.Env, alias string, expectedACL []yt.ACE) {
	var currentACL []yt.ACE
	for i := 0; i < 30; i++ {
		currentACL = getOpACL(t, env, alias)
		if reflect.DeepEqual(currentACL, expectedACL) {
			return
		}
		time.Sleep(time.Millisecond * 300)
	}

	currentACLString, err := yson.Marshal(currentACL)
	require.NoError(t, err)
	expectedACLString, err := yson.Marshal(expectedACL)
	require.NoError(t, err)
	env.L.Error("acl mismatch", log.String("current_acl", string(currentACLString)), log.String("expected_acl", string(expectedACLString)))

	t.FailNow()
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

	env.L.Debug("waiting for aliases", log.Strings("expected", expected))

	for i := 0; i < 30; i++ {
		actual := listAliases(t, env)
		sort.Strings(actual)

		if reflect.DeepEqual(expected, actual) {
			env.L.Debug("aliases has reached expected state", log.Strings("expected", expected))
			return
		}

		env.L.Debug("aliases has not reached expected state; waiting for the next try",
			log.Strings("expected", expected), log.Strings("actual", actual))

		time.Sleep(time.Millisecond * 300)
	}
	env.L.Error("aliases has not reached expected state in time", log.Strings("expected", expected))
	t.FailNow()
}

func waitIncarnation(t *testing.T, env *yttest.Env, alias string, expected int64) {
	env.L.Debug("waiting for alias incarnation",
		log.String("alias", alias),
		log.Int64("expected_incarnation", expected))
	for i := 0; i < 30; i++ {
		op := getOp(t, env, alias)
		if op != nil {
			annotations := op.RuntimeParameters.Annotations
			incarnation, ok := annotations["strawberry_incarnation"]
			require.True(t, ok)
			require.LessOrEqual(t, incarnation, expected)
			if reflect.DeepEqual(incarnation, expected) {
				env.L.Debug("alias has reached expected incarnation",
					log.String("alias", alias),
					log.Int64("incarnation", expected))
				return
			} else {
				env.L.Debug("operation has not reached expected incarnation; waiting for the next try",
					log.String("alias", alias),
					log.Any("actual_incarnation", incarnation),
					log.Int64("expected_incarnation", expected))
			}
		} else {
			env.L.Debug("operation is not found; waiting for the next try",
				log.String("alias", alias),
				log.Int64("incarnation", expected))
		}
		time.Sleep(time.Millisecond * 300)
	}
	env.L.Error("operation has not reached expected incarnation in time")
	t.FailNow()
}

func TestOperationBeforeStart(t *testing.T) {
	env, agent := prepare(t)
	t.Cleanup(agent.Stop)

	createStrawberryOp(t, env, "test1")
	agent.Start()
	defer agent.Stop()

	_ = waitOp(t, env, "test1")
}

func TestOperationAfterStart(t *testing.T) {
	env, agent := prepare(t)
	t.Cleanup(agent.Stop)

	agent.Start()
	defer agent.Stop()
	time.Sleep(time.Millisecond * 500)

	createStrawberryOp(t, env, "test2")

	_ = waitOp(t, env, "test2")
}

func TestAbortDangling(t *testing.T) {
	env, agent := prepare(t)
	t.Cleanup(agent.Stop)

	agent.Start()
	defer agent.Stop()

	createStrawberryOp(t, env, "test3")
	waitAliases(t, env, []string{"test3"})
	createStrawberryOp(t, env, "test4")
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
	defer agent.Stop()

	createStrawberryOp(t, env, "test5")
	waitIncarnation(t, env, "test5", 1)
	setSpecletOption(t, env, "test5", "test_option", "foo")
	waitIncarnation(t, env, "test5", 2)
	setSpecletOption(t, env, "test5", "test_option", "bar")
	waitIncarnation(t, env, "test5", 3)
	agent.Stop()
	waitIncarnation(t, env, "test5", 3)
	agent.Start()
	time.Sleep(time.Second * 2)
	waitIncarnation(t, env, "test5", 3)
	setSpecletOption(t, env, "test5", "test_option", "baz")
	waitIncarnation(t, env, "test5", 4)
}

func TestControllerStage(t *testing.T) {
	env, defaultAgent := prepare(t)
	defaultAgent.Start()
	defer defaultAgent.Stop()

	anotherAgent := createAgent(env, "another")
	anotherAgent.Start()
	defer anotherAgent.Stop()

	createStrawberryOp(t, env, "test6")
	waitIncarnation(t, env, "test6", 1)

	createStrawberryOp(t, env, "test7")
	waitIncarnation(t, env, "test7", 1)

	waitAliases(t, env, []string{"test6", "test7"})

	require.Equal(t, getOpStage(t, env, "test6"), "default")
	require.Equal(t, getOpStage(t, env, "test7"), "default")

	setSpecletOption(t, env, "test7", "stage", "unknown_stage")

	// There are no stage "unknown_stage", so no one keeps "test7" operation anymore.
	waitAliases(t, env, []string{"test6"})

	setSpecletOption(t, env, "test7", "stage", "another")

	waitAliases(t, env, []string{"test6", "test7"})

	require.Equal(t, getOpStage(t, env, "test6"), "default")
	require.Equal(t, getOpStage(t, env, "test7"), "another")

	defaultAgent.Stop()

	setSpecletOption(t, env, "test7", "dummy", rand.Uint64())
	setSpecletOption(t, env, "test6", "dummy", rand.Uint64())

	waitIncarnation(t, env, "test7", 3)
	// Default agent is off.
	waitIncarnation(t, env, "test6", 1)
}

func TestACLUpdate(t *testing.T) {
	env, agent := prepare(t)
	agent.Start()
	defer agent.Stop()

	createStrawberryOp(t, env, "test8")
	waitIncarnation(t, env, "test8", 1)

	defaultACL := []yt.ACE{
		{
			Action:      yt.ActionAllow,
			Subjects:    []string{"admins"},
			Permissions: []yt.Permission{yt.PermissionRead, yt.PermissionAdminister, yt.PermissionManage},
		},
		{
			Action:      yt.ActionAllow,
			Subjects:    []string{"root"},
			Permissions: []yt.Permission{yt.PermissionRead, yt.PermissionManage},
		},
	}
	realACL := getOpACL(t, env, "test8")

	require.True(t, reflect.DeepEqual(realACL, defaultACL))

	customACL := []yt.ACE{
		{
			Action:      yt.ActionAllow,
			Subjects:    []string{"everyone"},
			Permissions: []yt.Permission{yt.PermissionUse},
		},
	}
	setACL(t, env, "test8", customACL)

	// PermissionUse for strawberry op transforms to PermissionRead for YT op.
	// See toOperationACL.
	customACL[0].Permissions[0] = yt.PermissionRead

	// Default ACL is always appended to provided one.
	customACL = append(customACL, defaultACL...)

	waitOpACL(t, env, "test8", customACL)
}

func TestForceRestart(t *testing.T) {
	env, agent := prepare(t)
	agent.Start()
	defer agent.Stop()

	createStrawberryOp(t, env, "test9")
	waitIncarnation(t, env, "test9", 1)

	setSpecletOption(t, env, "test9", "dummy", rand.Uint64())
	waitIncarnation(t, env, "test9", 2)

	setSpecletOption(t, env, "test9", "restart_on_speclet_change", false)

	setSpecletOption(t, env, "test9", "min_speclet_revision", getOpletState(t, env, "test9").specletRevision)
	time.Sleep(time.Second * 1)
	// Operation should not be restarted because a speclet was not changed, so min_speclet_revision has no effect.
	waitIncarnation(t, env, "test9", 2)

	setSpecletOption(t, env, "test9", "dummy", rand.Uint64())

	time.Sleep(time.Second * 1)
	// Operation should not be restarted because restart_on_speclet_change is false.
	waitIncarnation(t, env, "test9", 2)

	setSpecletOption(t, env, "test9", "min_speclet_revision", getOpletState(t, env, "test9").specletRevision)
	// Now min_speclet_revision should be greater than operation_speclet_revision, so operation should be restarted.
	waitIncarnation(t, env, "test9", 3)

	setSpecletOption(t, env, "test9", "active", false)
	waitAliases(t, env, []string{})
}
