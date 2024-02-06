package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal"
)

func TestAuthClient(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "SetUserPassword", Test: suite.TestSetUserPassword, SkipRPC: true},
		{Name: "IssueListRevokeToken", Test: suite.TestIssueListRevokeToken, SkipRPC: true},
	})
}

func (s *Suite) TestSetUserPassword(t *testing.T, yc yt.Client) {
	user := "user-" + guid.New().String()
	_ = s.CreateUser(t, user)

	passwordAttr := ypath.Path.JoinChild("/", "sys", "users", user).Attr("hashed_password")
	exists, err := yc.NodeExists(s.Ctx, passwordAttr, nil)
	require.NoError(t, err)
	require.False(t, exists)

	err = yc.SetUserPassword(s.Ctx, user, "brabu", "", nil)
	require.NoError(t, err)

	exists, err = yc.NodeExists(s.Ctx, passwordAttr, nil)
	require.NoError(t, err)
	require.True(t, exists)

	var hashedPassword string
	err = yc.GetNode(s.Ctx, passwordAttr, &hashedPassword, nil)
	require.NoError(t, err)
	require.NotEmpty(t, hashedPassword)
}

func (s *Suite) TestIssueListRevokeToken(t *testing.T, yc yt.Client) {
	user := "user-" + guid.New().String()
	_ = s.CreateUser(t, user)

	token, err := yc.IssueToken(s.Ctx, user, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	tokens, err := yc.ListUserTokens(s.Ctx, user, "", nil)
	require.NoError(t, err)
	tokenSHA := internal.EncodeSHA256(token)
	require.Contains(t, tokens, tokenSHA)

	err = yc.RevokeToken(s.Ctx, user, "", token, nil)
	require.NoError(t, err)

	tokens, err = yc.ListUserTokens(s.Ctx, user, "", nil)
	require.NoError(t, err)
	require.Empty(t, tokens)
}
