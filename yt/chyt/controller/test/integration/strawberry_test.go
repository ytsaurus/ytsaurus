package integration

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func createStrawberryOp(t *testing.T, env *helpers.Env, alias string) {
	env.L.Debug("creating node", log.String("alias", alias))
	_, err := env.YT.CreateNode(env.Ctx, env.StrawberryRoot.Child(alias), yt.NodeMap, nil)
	require.NoError(t, err)
	_, err = env.YT.CreateObject(env.Ctx, yt.NodeAccessControlObject, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name":      alias,
			"namespace": "sleep",
		},
	})
	require.NoError(t, err)
	_, err = env.YT.CreateNode(env.Ctx, env.StrawberryRoot.Child(alias).Child("speclet"), yt.NodeDocument, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"value": map[string]any{
				"active":                    true,
				"family":                    "sleep",
				"stage":                     "default",
				"restart_on_speclet_change": true,
				"pool":                      "test_pool",
			},
		},
	})
	require.NoError(t, err)
}

func removeNode(t *testing.T, env *helpers.Env, alias string) {
	env.L.Debug("removing node", log.String("alias", alias))
	err := env.YT.RemoveNode(
		env.Ctx,
		env.StrawberryRoot.Child(alias),
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

func getOpletState(t *testing.T, env *helpers.Env, alias string) opletState {
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

	err := env.YT.GetNode(env.Ctx, env.StrawberryRoot.Child(alias), &node, &yt.GetNodeOptions{Attributes: attributes})
	require.NoError(t, err)

	return opletState{
		node.PersistentState,
		node.InfoState,
		node.Revision,
		node.Speclet.Value,
		node.Speclet.Revision,
	}
}

func setAttr(t *testing.T, env *helpers.Env, alias string, attr string, value any) {
	env.L.Debug("setting attribute", log.String("attr", alias+"/@"+attr))
	err := env.YT.SetNode(env.Ctx, env.StrawberryRoot.Child(alias).Attr(attr), value, nil)
	require.NoError(t, err)
}

func setSpecletOption(t *testing.T, env *helpers.Env, alias string, option string, value any) {
	env.L.Debug("setting speclet option", log.String("alias", alias), log.String("option", option), log.Any("value", value))
	err := env.YT.SetNode(env.Ctx, env.StrawberryRoot.JoinChild(alias, "speclet", option), value, nil)
	require.NoError(t, err)
}

func setACL(t *testing.T, env *helpers.Env, alias string, acl []yt.ACE) {
	aclPath := strawberry.AccessControlNamespacesPath.JoinChild("sleep", alias).Attr("principal_acl")
	err := env.YT.SetNode(env.Ctx, aclPath, acl, nil)
	require.NoError(t, err)
}

func getOp(t *testing.T, env *helpers.Env, alias string) *yt.OperationStatus {
	t.Helper()

	ops, err := yt.ListAllOperations(env.Ctx, env.YT, nil)
	require.NoError(t, err)
	for _, op := range ops {
		if opAlias, ok := op.BriefSpec["alias"].(string); ok && opAlias == "*"+alias {
			return &op
		}
	}
	return nil
}

func getOpStage(t *testing.T, env *helpers.Env, alias string) string {
	op := getOp(t, env, alias)
	require.NotEqual(t, op, nil)
	annotations := op.RuntimeParameters.Annotations
	stage, ok := annotations["strawberry_stage"]
	require.True(t, ok)
	return stage.(string)
}

func waitOp(t *testing.T, env *helpers.Env, alias string) *yt.OperationStatus {
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

func getOpACL(t *testing.T, env *helpers.Env, alias string) []yt.ACE {
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

func waitOpACL(t *testing.T, env *helpers.Env, alias string, expectedACL []yt.ACE) {
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

func listAliases(t *testing.T, env *helpers.Env) []string {
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

func waitAliases(t *testing.T, env *helpers.Env, expected []string) {
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

func waitIncarnation(t *testing.T, env *helpers.Env, alias string, expected int64) {
	// tag is used for easier log grep.
	tag := rand.Uint64()

	env.L.Debug("waiting for alias incarnation",
		log.String("alias", alias),
		log.Int64("expected_incarnation", expected),
		log.UInt64("tag", tag))
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
					log.Int64("incarnation", expected),
					log.UInt64("tag", tag))
				return
			} else {
				env.L.Debug("operation has not reached expected incarnation; waiting for the next try",
					log.String("alias", alias),
					log.Any("actual_incarnation", incarnation),
					log.Int64("expected_incarnation", expected),
					log.UInt64("tag", tag))
			}
		} else {
			env.L.Debug("operation is not found; waiting for the next try",
				log.String("alias", alias),
				log.Int64("incarnation", expected),
				log.UInt64("tag", tag))
		}
		time.Sleep(time.Millisecond * 300)
	}
	env.L.Error("operation has not reached expected incarnation in time",
		log.UInt64("tag", tag))
	t.FailNow()
}

func TestOperationBeforeStart(t *testing.T) {
	env, agent := helpers.PrepareAgent(t)
	t.Cleanup(agent.Stop)

	createStrawberryOp(t, env, "test1")
	agent.Start()
	defer agent.Stop()

	_ = waitOp(t, env, "test1")
}

func TestOperationAfterStart(t *testing.T) {
	env, agent := helpers.PrepareAgent(t)
	t.Cleanup(agent.Stop)

	agent.Start()
	defer agent.Stop()
	time.Sleep(time.Millisecond * 500)

	createStrawberryOp(t, env, "test2")

	_ = waitOp(t, env, "test2")
}

func TestAbortDangling(t *testing.T) {
	env, agent := helpers.PrepareAgent(t)
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
	env, agent := helpers.PrepareAgent(t)
	t.Cleanup(agent.Stop)

	agent.Start()
	defer agent.Stop()

	createStrawberryOp(t, env, "test5")
	waitIncarnation(t, env, "test5", 1)
	setSpecletOption(t, env, "test5", "test_option", 1)
	waitIncarnation(t, env, "test5", 2)
	setSpecletOption(t, env, "test5", "test_option", 2)
	waitIncarnation(t, env, "test5", 3)
	agent.Stop()
	waitIncarnation(t, env, "test5", 3)
	agent.Start()
	time.Sleep(time.Second * 2)
	waitIncarnation(t, env, "test5", 3)
	setSpecletOption(t, env, "test5", "test_option", 3)
	waitIncarnation(t, env, "test5", 4)
}

func TestControllerStage(t *testing.T) {
	env, defaultAgent := helpers.PrepareAgent(t)
	defaultAgent.Start()
	defer defaultAgent.Stop()

	anotherAgent := helpers.CreateAgent(env, "another")
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

	setSpecletOption(t, env, "test7", "test_option", 1234)
	setSpecletOption(t, env, "test6", "test_option", 4321)

	waitIncarnation(t, env, "test7", 3)
	// Default agent is off.
	waitIncarnation(t, env, "test6", 1)
}

func TestACLUpdate(t *testing.T) {
	env, agent := helpers.PrepareAgent(t)
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
	env, agent := helpers.PrepareAgent(t)
	agent.Start()
	defer agent.Stop()

	createStrawberryOp(t, env, "test9")
	waitIncarnation(t, env, "test9", 1)

	setSpecletOption(t, env, "test9", "test_option", 1234)
	waitIncarnation(t, env, "test9", 2)

	setSpecletOption(t, env, "test9", "restart_on_speclet_change", false)

	setSpecletOption(t, env, "test9", "test_option", 4321)
	time.Sleep(time.Second * 1)
	// Operation should not be restarted because restart_on_speclet_change is false.
	waitIncarnation(t, env, "test9", 2)

	setSpecletOption(t, env, "test9", "min_speclet_revision", getOpletState(t, env, "test9").specletRevision)
	// Now min_speclet_revision should be greater than operation_speclet_revision, so operation should be restarted.
	waitIncarnation(t, env, "test9", 3)

	setSpecletOption(t, env, "test9", "active", false)
	waitAliases(t, env, []string{})
}

func TestControllerUpdate(t *testing.T) {
	env, agent := helpers.PrepareAgent(t)
	agent.Start()
	defer agent.Stop()

	createStrawberryOp(t, env, "test10")
	waitIncarnation(t, env, "test10", 1)

	err := env.YT.SetNode(env.Ctx, env.StrawberryRoot.Attr("controller_parameter"), "new_parameter", nil)
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 400)
	waitIncarnation(t, env, "test10", 2)
}
