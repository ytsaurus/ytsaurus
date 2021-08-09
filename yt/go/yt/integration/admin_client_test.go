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
		{"AddRemoveMember", suite.TestAddRemoveMember},
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
