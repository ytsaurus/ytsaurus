package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestAdminClient(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "AddRemoveMember", Test: suite.TestAddRemoveMember},
		{Name: "AddRemoveMaintenance", Test: suite.TestAddRemoveMaintenance},
		{Name: "TransferAccountResources", Test: suite.TestTransferAccountResources},
		{Name: "TransferPoolResources", Test: suite.TestTransferPoolResources, SkipRPC: true},
		{Name: "CheckPermission", Test: suite.TestCheckPermission},
		{Name: "CheckPermissionByACL", Test: suite.TestCheckPermissionByACL},
		{Name: "CheckColumnPermission", Test: suite.TestCheckColumnPermission},
		{Name: "BuildMasterSnapshots", Test: suite.TestBuildMasterSnapshots},
		{Name: "BuildSnapshot", Test: suite.TestBuildSnapshot},
	})
}

func (s *Suite) TestAddRemoveMember(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	group := "group-" + guid.New().String()
	_ = s.CreateGroup(ctx, t, group)

	user := "user-" + guid.New().String()
	_ = s.CreateUser(ctx, t, user)

	require.False(t, s.MemberOf(ctx, t, user, group))

	err := yc.AddMember(ctx, group, user, nil)
	require.NoError(t, err)
	require.True(t, s.MemberOf(ctx, t, user, group))

	err = yc.RemoveMember(ctx, group, user, nil)
	require.NoError(t, err)
	require.False(t, s.MemberOf(ctx, t, user, group))
}

func (s *Suite) TestAddRemoveMaintenance(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	// Get node
	var nodes []string
	require.NoError(t, yc.ListNode(ctx, ypath.Path("//sys/cluster_nodes"), &nodes, nil))
	node := nodes[0]

	// Check count of maintenance requests
	checkReqs := func(expectedLen int) {
		t.Helper()

		requestsPath := ypath.Path("//sys/cluster_nodes").Child(node).Attr("maintenance_requests")

		var reqs map[string]any
		require.NoError(t, yc.GetNode(ctx, requestsPath, &reqs, nil))
		require.Len(t, reqs, expectedLen)
	}

	// Adding new maintenance requests
	addResp, err := yc.AddMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, yt.MaintenanceTypePendingRestart, "restart1", nil)
	require.NoError(t, err)
	_, err = yc.AddMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, yt.MaintenanceTypePendingRestart, "restart2", nil)
	require.NoError(t, err)
	_, err = yc.AddMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, yt.MaintenanceTypePendingRestart, "restart3", nil)
	require.NoError(t, err)

	checkReqs(3)

	// Removing maintenance with comment "restart1"
	resp, err := yc.RemoveMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, &yt.RemoveMaintenanceOptions{
		IDs: []yt.MaintenanceID{addResp.ID},
	})
	require.NoError(t, err)
	require.Equal(t, yt.RemoveMaintenanceResponse{
		PendingRestartCounts: 1,
	}, *resp)

	checkReqs(2)

	// Check handling error on empty options
	_, err = yc.RemoveMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, nil)
	require.Error(t, err)

	// Removing remaining maintenances
	resp, err = yc.RemoveMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, &yt.RemoveMaintenanceOptions{
		Mine: ptr.Bool(true),
	})
	require.NoError(t, err)
	require.Equal(t, yt.RemoveMaintenanceResponse{
		PendingRestartCounts: 2,
	}, *resp)

	checkReqs(0)
}

func (s *Suite) TestTransferAccountResources(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	mebibyte := 1024 * 1024
	from := "account-from-" + guid.New().String()
	_ = s.CreateAccount(ctx, t, from, map[string]any{
		"resource_limits": map[string]any{
			"disk_space_per_medium": map[string]any{
				"default": 10 * mebibyte,
			},
			"node_count":  20,
			"chunk_count": 30,
		},
	})

	to := "account-to-" + guid.New().String()
	_ = s.CreateAccount(ctx, t, to, map[string]any{
		"resource_limits": map[string]any{
			"disk_space_per_medium": map[string]any{
				"default": 5 * mebibyte,
			},
			"node_count":  10,
			"chunk_count": 15,
		},
	})

	err := yc.TransferAccountResources(ctx, from, to, map[string]any{
		"disk_space_per_medium": map[string]any{
			"default": 3 * mebibyte,
		},
		"node_count":  6,
		"chunk_count": 9,
	}, nil)

	require.NoError(t, err)

	require.Equal(t, 7*mebibyte, s.getAccountResourceAttribute(ctx, t, yc, from, "disk_space_per_medium/default"))
	require.Equal(t, 14, s.getAccountResourceAttribute(ctx, t, yc, from, "node_count"))
	require.Equal(t, 21, s.getAccountResourceAttribute(ctx, t, yc, from, "chunk_count"))

	require.Equal(t, 8*mebibyte, s.getAccountResourceAttribute(ctx, t, yc, to, "disk_space_per_medium/default"))
	require.Equal(t, 16, s.getAccountResourceAttribute(ctx, t, yc, to, "node_count"))
	require.Equal(t, 24, s.getAccountResourceAttribute(ctx, t, yc, to, "chunk_count"))
}

func (s *Suite) TestTransferPoolResources(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	poolTree := "tree-" + guid.New().String()
	_ = s.CreatePoolTree(ctx, t, poolTree)

	from := "pool-from-" + guid.New().String()
	_ = s.CreatePool(ctx, t, from, poolTree, map[string]any{
		"strong_guarantee_resources": map[string]any{"cpu": 10},
		"integral_guarantees": map[string]any{
			"resource_flow":             map[string]any{"cpu": 20},
			"burst_guarantee_resources": map[string]any{"cpu": 30},
		},
		"max_running_operation_count": 40,
		"max_operation_count":         50,
	})

	to := "pool-to-" + guid.New().String()
	_ = s.CreatePool(ctx, t, to, poolTree, map[string]any{
		"strong_guarantee_resources": map[string]any{"cpu": 10},
		"integral_guarantees": map[string]any{
			"resource_flow":             map[string]any{"cpu": 20},
			"burst_guarantee_resources": map[string]any{"cpu": 30},
		},
		"max_running_operation_count": 40,
		"max_operation_count":         50,
	})

	err := yc.TransferPoolResources(ctx, from, to, poolTree, map[string]any{
		"strong_guarantee_resources":  map[string]any{"cpu": 4},
		"resource_flow":               map[string]any{"cpu": 8},
		"burst_guarantee_resources":   map[string]any{"cpu": 12},
		"max_running_operation_count": 16,
		"max_operation_count":         20,
	}, nil)

	require.NoError(t, err)
	require.Equal(t, 6.0, s.getPoolAttribute(ctx, t, yc, poolTree, from, "strong_guarantee_resources/cpu"))
	require.Equal(t, 12.0, s.getPoolAttribute(ctx, t, yc, poolTree, from, "integral_guarantees/resource_flow/cpu"))
	require.Equal(t, 18.0, s.getPoolAttribute(ctx, t, yc, poolTree, from, "integral_guarantees/burst_guarantee_resources/cpu"))
	require.Equal(t, 24.0, s.getPoolAttribute(ctx, t, yc, poolTree, from, "max_running_operation_count"))
	require.Equal(t, 30.0, s.getPoolAttribute(ctx, t, yc, poolTree, from, "max_operation_count"))

	require.Equal(t, 14.0, s.getPoolAttribute(ctx, t, yc, poolTree, to, "strong_guarantee_resources/cpu"))
	require.Equal(t, 28.0, s.getPoolAttribute(ctx, t, yc, poolTree, to, "integral_guarantees/resource_flow/cpu"))
	require.Equal(t, 42.0, s.getPoolAttribute(ctx, t, yc, poolTree, to, "integral_guarantees/burst_guarantee_resources/cpu"))
	require.Equal(t, 56.0, s.getPoolAttribute(ctx, t, yc, poolTree, to, "max_running_operation_count"))
	require.Equal(t, 70.0, s.getPoolAttribute(ctx, t, yc, poolTree, to, "max_operation_count"))
}

func (s *Suite) getPoolAttribute(ctx context.Context, t *testing.T, yc yt.Client, poolTree string, pool string, attribute string) float64 {
	t.Helper()
	var result float64
	path := ypath.Path("//sys/pool_trees").Child(poolTree).Child(pool).Attr(attribute)
	require.NoError(t, yc.GetNode(ctx, path, &result, nil))
	return result
}

func (s *Suite) getAccountResourceAttribute(ctx context.Context, t *testing.T, yc yt.Client, account string, attribute string) int {
	t.Helper()
	var result int
	path := ypath.Path("//sys/accounts").Child(account).Attr("resource_limits").Child(attribute)
	require.NoError(t, yc.GetNode(ctx, path, &result, nil))
	return result
}

func (s *Suite) CreateGroup(ctx context.Context, t *testing.T, name string) yt.NodeID {
	t.Helper()

	id, err := s.YT.CreateObject(ctx, yt.NodeGroup, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": name,
		}})

	require.NoError(t, err)
	return id
}

func (s *Suite) CreateUser(ctx context.Context, t *testing.T, user string) yt.NodeID {
	t.Helper()

	id, err := s.YT.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": user,
		},
	})

	require.NoError(t, err)
	return id
}

func (s *Suite) MemberOf(ctx context.Context, t *testing.T, user, group string) bool {
	t.Helper()

	var groups []string
	err := s.YT.GetNode(ctx, ypath.Path("//sys/users").Child(user).Attr("member_of"), &groups, nil)
	require.NoError(t, err)

	for _, g := range groups {
		if g == group {
			return true
		}
	}

	return false
}

func (s *Suite) CreateAccount(ctx context.Context, t *testing.T, accountName string, attributes map[string]any) yt.NodeID {
	t.Helper()

	attributes["name"] = accountName
	id, err := s.YT.CreateObject(ctx, yt.NodeAccount, &yt.CreateObjectOptions{
		Attributes: attributes,
	})

	require.NoError(t, err)
	return id
}

func (s *Suite) CreatePool(ctx context.Context, t *testing.T, poolName string, poolTree string, attributes map[string]any) yt.NodeID {
	t.Helper()

	attributes["name"] = poolName
	attributes["pool_tree"] = poolTree
	id, err := s.YT.CreateObject(ctx, yt.NodeSchedulerPool, &yt.CreateObjectOptions{
		Attributes: attributes,
	})

	require.NoError(t, err)
	return id
}

func (s *Suite) CreatePoolTree(ctx context.Context, t *testing.T, poolTreeName string) yt.NodeID {
	t.Helper()

	id, err := s.YT.CreateObject(ctx, yt.NodeSchedulerPoolTree, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": poolTreeName,
		},
	})

	require.NoError(t, err)
	return id
}

func (s *Suite) TestCheckPermission(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	user := guid.New().String()
	userID, err := yc.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": user,
		},
	})
	require.NoError(t, err)

	permissions := []yt.Permission{
		yt.PermissionRead,
		yt.PermissionWrite,
		yt.PermissionUse,
		yt.PermissionAdminister,
		yt.PermissionCreate,
		yt.PermissionRemove,
		yt.PermissionMount,
		yt.PermissionManage,
		yt.PermissionModifyChildren,
	}

	for _, nodePermission := range permissions {
		p := tmpPath()
		nodeID, err := yc.CreateNode(ctx, p, yt.NodeMap, &yt.CreateNodeOptions{
			Attributes: map[string]any{
				"acl": []yt.ACE{
					{
						Action:      yt.ActionAllow,
						Subjects:    []string{user},
						Permissions: []yt.Permission{nodePermission},
					},
				},
				"inherit_acl": false,
			},
		})
		require.NoError(t, err)

		for _, userPermission := range permissions {
			response, err := yc.CheckPermission(ctx, user, userPermission, p, nil)
			require.NoError(t, err)

			if userPermission == nodePermission {
				expectedResponse := &yt.CheckPermissionResponse{
					CheckPermissionResult: yt.CheckPermissionResult{
						Action:      yt.ActionAllow,
						ObjectID:    nodeID,
						ObjectName:  ptr.String("node " + p.YPath().String()),
						SubjectID:   userID,
						SubjectName: &user,
					},
				}
				require.Equal(t, expectedResponse, response)
			} else {
				expectedResponse := &yt.CheckPermissionResponse{
					CheckPermissionResult: yt.CheckPermissionResult{
						Action: yt.ActionDeny,
					},
				}
				require.Equal(t, expectedResponse, response)
			}
		}
	}
}

func (s *Suite) TestCheckPermissionByACL(ctx context.Context, t *testing.T, c yt.Client) {
	user := "user-" + guid.New().String()
	_ = s.CreateUser(ctx, t, user)

	response, err := c.CheckPermissionByACL(ctx, user, yt.PermissionRead, []yt.ACE{
		{
			Action:          yt.ActionAllow,
			Permissions:     []yt.Permission{yt.PermissionRead},
			Subjects:        []string{user},
			InheritanceMode: "object_only",
		},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, yt.ActionAllow, response.Action)

	response, err = c.CheckPermissionByACL(ctx, user, yt.PermissionRead, []yt.ACE{
		{
			Action:          yt.ActionDeny,
			Permissions:     []yt.Permission{yt.PermissionRead},
			Subjects:        []string{user},
			InheritanceMode: "object_only",
		},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, yt.ActionDeny, response.Action)
}

func (s *Suite) TestCheckColumnPermission(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	user1 := guid.New().String()
	user1ID, err := yc.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": user1,
		},
	})

	require.NoError(t, err)
	user2 := guid.New().String()
	_, err = yc.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": user2,
		},
	})
	require.NoError(t, err)

	var usersGroupID yt.NodeID
	err = yc.GetNode(ctx, ypath.Path("//sys/groups/users/@id"), &usersGroupID, nil)
	require.NoError(t, err)

	p := tmpPath()
	nodeID, err := yc.CreateNode(ctx, p, yt.NodeMap, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"acl": []yt.ACE{
				{
					Action:      yt.ActionAllow,
					Subjects:    []string{"users"},
					Permissions: []yt.Permission{yt.PermissionRead},
				},
				{
					Action:      yt.ActionAllow,
					Subjects:    []string{user1},
					Permissions: []yt.Permission{yt.PermissionRead},
					Columns:     []string{"protected_column"},
				},
			},
			"inherit_acl": false,
		},
	})
	require.NoError(t, err)

	allowUsersResult := yt.CheckPermissionResult{
		Action:      yt.ActionAllow,
		ObjectID:    nodeID,
		ObjectName:  ptr.String("node " + p.YPath().String()),
		SubjectID:   usersGroupID,
		SubjectName: ptr.String("users"),
	}

	// check user1's permissions

	response, err := yc.CheckPermission(ctx, user1, yt.PermissionRead, p, &yt.CheckPermissionOptions{
		Columns: []string{"protected_column", "regular_column"},
	})
	require.NoError(t, err)

	expectedResponse := &yt.CheckPermissionResponse{
		CheckPermissionResult: allowUsersResult,
		Columns: []yt.CheckPermissionResult{
			{
				Action:      yt.ActionAllow,
				ObjectID:    nodeID,
				ObjectName:  ptr.String("node " + p.YPath().String()),
				SubjectID:   user1ID,
				SubjectName: &user1,
			},
			allowUsersResult,
		},
	}

	require.Equal(t, expectedResponse, response)

	// check user2's permission

	response, err = yc.CheckPermission(ctx, user2, yt.PermissionRead, p, &yt.CheckPermissionOptions{
		Columns: []string{"protected_column", "regular_column"},
	})
	require.NoError(t, err)

	expectedResponse = &yt.CheckPermissionResponse{
		CheckPermissionResult: allowUsersResult,
		Columns: []yt.CheckPermissionResult{
			{
				Action: yt.ActionDeny,
			},
			allowUsersResult,
		},
	}

	require.Equal(t, expectedResponse, response)
}

func (s *Suite) TestBuildMasterSnapshots(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	response, err := yc.BuildMasterSnapshots(ctx, nil)
	require.NoError(t, err)
	require.NotEmpty(t, response)
}

func (s *Suite) TestBuildSnapshot(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	var cellID guid.GUID
	err := yc.GetNode(ctx, ypath.Path("//sys/@cluster_connection/primary_master/cell_id"), &cellID, nil)
	require.NoError(t, err)

	response, err := yc.BuildSnapshot(ctx, &yt.BuildSnapshotOptions{CellID: &cellID})
	require.NoError(t, err)
	require.NotEmpty(t, response)
}
