package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

func TestAdminClient(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "AddRemoveMember", Test: suite.TestAddRemoveMember},
		{Name: "TransferPoolResources", Test: suite.TestTransferPoolResources, SkipRPC: true},
	})
}

func (s *Suite) TestAddRemoveMember(t *testing.T, yc yt.Client) {
	t.Parallel()

	group := "group-" + guid.New().String()
	_ = s.CreateGroup(t, group)

	user := "user-" + guid.New().String()
	_ = s.CreateUser(t, user)

	require.False(t, s.MemberOf(t, user, group))

	err := yc.AddMember(s.Ctx, group, user, nil)
	require.NoError(t, err)
	require.True(t, s.MemberOf(t, user, group))

	err = yc.RemoveMember(s.Ctx, group, user, nil)
	require.NoError(t, err)
	require.False(t, s.MemberOf(t, user, group))
}

func (s *Suite) TestTransferPoolResources(t *testing.T, yc yt.Client) {
	t.Parallel()

	// TODO(renadeen) REMOVE SKIP WHEN YT BINARIES WILL BE UPDATED IN ARC.
	t.Skip()

	poolTree := "tree-" + guid.New().String()
	_ = s.CreatePoolTree(t, poolTree)

	from := "pool-from-" + guid.New().String()
	_ = s.CreatePool(t, from, poolTree, map[string]interface{}{
		"strong_guarantee_resources": map[string]interface{}{"cpu": 10},
		"integral_guarantees": map[string]interface{}{
			"resource_flow":             map[string]interface{}{"cpu": 20},
			"burst_guarantee_resources": map[string]interface{}{"cpu": 30},
		},
		"max_running_operation_count": 40,
		"max_operation_count":         50,
	})

	to := "pool-to-" + guid.New().String()
	_ = s.CreatePool(t, to, poolTree, map[string]interface{}{
		"strong_guarantee_resources": map[string]interface{}{"cpu": 10},
		"integral_guarantees": map[string]interface{}{
			"resource_flow":             map[string]interface{}{"cpu": 20},
			"burst_guarantee_resources": map[string]interface{}{"cpu": 30},
		},
		"max_running_operation_count": 40,
		"max_operation_count":         50,
	})

	err := yc.TransferPoolResources(s.Ctx, from, to, poolTree, map[string]interface{}{
		"strong_guarantee_resources":  map[string]interface{}{"cpu": 4},
		"resource_flow":               map[string]interface{}{"cpu": 8},
		"burst_guarantee_resources":   map[string]interface{}{"cpu": 12},
		"max_running_operation_count": 16,
		"max_operation_count":         20,
	}, nil)

	require.NoError(t, err)
	require.Equal(t, 6.0, s.getDoubleAttribute(t, yc, poolTree, from, "strong_guarantee_resources/cpu"))
	require.Equal(t, 12.0, s.getDoubleAttribute(t, yc, poolTree, from, "integral_guarantees/resource_flow/cpu"))
	require.Equal(t, 18.0, s.getDoubleAttribute(t, yc, poolTree, from, "integral_guarantees/burst_guarantee_resources/cpu"))
	require.Equal(t, 24.0, s.getDoubleAttribute(t, yc, poolTree, from, "max_running_operation_count"))
	require.Equal(t, 30.0, s.getDoubleAttribute(t, yc, poolTree, from, "max_operation_count"))

	require.Equal(t, 14.0, s.getDoubleAttribute(t, yc, poolTree, to, "strong_guarantee_resources/cpu"))
	require.Equal(t, 28.0, s.getDoubleAttribute(t, yc, poolTree, to, "integral_guarantees/resource_flow/cpu"))
	require.Equal(t, 42.0, s.getDoubleAttribute(t, yc, poolTree, to, "integral_guarantees/burst_guarantee_resources/cpu"))
	require.Equal(t, 56.0, s.getDoubleAttribute(t, yc, poolTree, to, "max_running_operation_count"))
	require.Equal(t, 70.0, s.getDoubleAttribute(t, yc, poolTree, to, "max_operation_count"))
}

func (s *Suite) getDoubleAttribute(t *testing.T, yc yt.Client, poolTree string, pool string, attribute string) float64 {
	var result float64
	require.NoError(t, yc.GetNode(
		s.Ctx,
		ypath.Path("//sys/pool_trees/"+poolTree+"/"+pool).Attr(attribute),
		&result,
		nil))
	return result
}

func (s *Suite) CreateGroup(t *testing.T, name string) yt.NodeID {
	t.Helper()

	id, err := s.YT.CreateObject(s.Ctx, yt.NodeGroup, &yt.CreateObjectOptions{
		Attributes: map[string]interface{}{
			"name": name,
		}})

	require.NoError(t, err)
	return id
}

func (s *Suite) CreateUser(t *testing.T, user string) yt.NodeID {
	t.Helper()

	id, err := s.YT.CreateObject(s.Ctx, yt.NodeUser, &yt.CreateObjectOptions{
		Attributes: map[string]interface{}{
			"name": user,
		},
	})

	require.NoError(t, err)
	return id
}

func (s *Suite) MemberOf(t *testing.T, user, group string) bool {
	t.Helper()

	var groups []string
	err := s.YT.GetNode(s.Ctx, ypath.Path("//sys/users").Child(user).Attr("member_of"), &groups, nil)
	require.NoError(t, err)

	for _, g := range groups {
		if g == group {
			return true
		}
	}

	return false
}

func (s *Suite) CreatePool(t *testing.T, poolName string, poolTree string, attributes map[string]interface{}) yt.NodeID {
	t.Helper()

	attributes["name"] = poolName
	attributes["pool_tree"] = poolTree
	id, err := s.YT.CreateObject(s.Ctx, yt.NodeSchedulerPool, &yt.CreateObjectOptions{
		Attributes: attributes,
	})

	require.NoError(t, err)
	return id
}

func (s *Suite) CreatePoolTree(t *testing.T, poolTreeName string) yt.NodeID {
	t.Helper()

	id, err := s.YT.CreateObject(s.Ctx, yt.NodeSchedulerPoolTree, &yt.CreateObjectOptions{
		Attributes: map[string]interface{}{
			"name": poolTreeName,
		},
	})

	require.NoError(t, err)
	return id
}
