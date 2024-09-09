package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

func TestAuthClient(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "SetUserPassword", Test: suite.TestSetUserPassword, SkipRPC: true},
		{Name: "IssueListRevokeToken", Test: suite.TestIssueListRevokeToken, SkipRPC: true},
		{Name: "WhoAmI", Test: suite.TestWhoAmI, SkipRPC: true},
	})
}

func (s *Suite) TestSetUserPassword(ctx context.Context, t *testing.T, yc yt.Client) {
	user := "user-" + guid.New().String()
	_ = s.CreateUser(ctx, t, user)

	passwordAttr := ypath.Path.JoinChild("/", "sys", "users", user).Attr("hashed_password")
	exists, err := yc.NodeExists(ctx, passwordAttr, nil)
	require.NoError(t, err)
	require.False(t, exists)

	err = yc.SetUserPassword(ctx, user, "brabu", "", nil)
	require.NoError(t, err)

	exists, err = yc.NodeExists(ctx, passwordAttr, nil)
	require.NoError(t, err)
	require.True(t, exists)

	var hashedPassword string
	err = yc.GetNode(ctx, passwordAttr, &hashedPassword, nil)
	require.NoError(t, err)
	require.NotEmpty(t, hashedPassword)
}

func (s *Suite) TestIssueListRevokeToken(ctx context.Context, t *testing.T, yc yt.Client) {
	user := "user-" + guid.New().String()
	_ = s.CreateUser(ctx, t, user)

	token, err := yc.IssueToken(ctx, user, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	tokens, err := yc.ListUserTokens(ctx, user, "", nil)
	require.NoError(t, err)
	tokenSHA := encodeSHA256(token)
	require.Contains(t, tokens, tokenSHA)

	err = yc.RevokeToken(ctx, user, "", token, nil)
	require.NoError(t, err)

	tokens, err = yc.ListUserTokens(ctx, user, "", nil)
	require.NoError(t, err)
	require.Empty(t, tokens)
}

func (s *Suite) TestWhoAmI(ctx context.Context, t *testing.T, yc yt.Client) {
	user := "user-" + guid.New().String()
	_ = s.CreateUser(ctx, t, user)
	token, err := yc.IssueToken(ctx, user, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	userClient, err := ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: s.Env.L, Token: token})
	require.NoError(t, err)
	res, err := userClient.WhoAmI(ctx, nil)
	require.NoError(t, err)

	// TODO(wilwell) Remove "root" from the acceptable options. It's either "root" if auth is disabled, or user otherwise.
	require.Contains(t, []string{"root", user}, res.Login)
}

func encodeSHA256(input string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(input)))
}
