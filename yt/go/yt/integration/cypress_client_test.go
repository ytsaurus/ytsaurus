package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func TestCypressClient(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "CreateNode", Test: suite.TestCreateNode},
		{Name: "NodeExists", Test: suite.TestNodeExists},
		{Name: "RemoveNode", Test: suite.TestRemoveNode},
		{Name: "GetNode", Test: suite.TestGetNode},
		{Name: "SetNode", Test: suite.TestSetNode},
		{Name: "ListNode", Test: suite.TestListNode},
		{Name: "MultisetAttributes", Test: suite.TestMultisetAttributes},
		{Name: "CopyNode", Test: suite.TestCopyNode},
		{Name: "MoveNode", Test: suite.TestMoveNode},
		{Name: "LinkNode", Test: suite.TestLinkNode},
		{Name: "CreateObject", Test: suite.TestCreateObject},
		{Name: "BinaryPath", Test: suite.TestBinaryPath},
	})
}

func (s *Suite) TestCreateNode(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	p := tmpPath()

	id, err := yc.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
		Recursive: true,
	})
	require.NoError(t, err)

	ok, err := yc.NodeExists(ctx, p, nil)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = yc.NodeExists(ctx, id.YPath(), nil)
	require.NoError(t, err)
	require.True(t, ok)
}

func (s *Suite) TestNodeExists(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	ok, err := yc.NodeExists(ctx, ypath.Path("/"), nil)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = yc.NodeExists(ctx, tmpPath(), nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func (s *Suite) TestRemoveNode(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	root := tmpPath()

	err := yc.RemoveNode(ctx, root, nil)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.ErrorCode(500)))

	p := root.Child(guid.New().String())

	_, err = yc.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
		Recursive: true,
	})
	require.NoError(t, err)

	err = yc.RemoveNode(ctx, root, &yt.RemoveNodeOptions{
		Recursive: true,
		Force:     true,
	})
	require.NoError(t, err)

	ok, err := yc.NodeExists(ctx, root, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func (s *Suite) TestGetNode(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	var attrs struct {
		Account  string `yson:"account"`
		Revision uint64 `yson:"revision"`
	}

	err := yc.GetNode(ctx, ypath.Path("//@"), &attrs, nil)
	require.NoError(t, err)

	require.NotEqual(t, "", attrs.Account)
	require.NotZero(t, attrs.Revision)
}

func (s *Suite) TestSetNode(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	p := ypath.Path("/").Attr(guid.New().String())

	value := "Gopher!"
	err := yc.SetNode(ctx, p, value, nil)
	require.NoError(t, err)

	var cypressValue string
	err = yc.GetNode(ctx, p, &cypressValue, nil)
	require.NoError(t, err)
	require.Equal(t, value, cypressValue)
}

func (s *Suite) TestListNode(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	var list []struct {
		Owner string `yson:"owner,attr"`
		Name  string `yson:",value"`
	}

	err := yc.ListNode(ctx, ypath.Path("/"), &list, &yt.ListNodeOptions{
		Attributes: []string{"owner"},
	})
	require.NoError(t, err)

	require.NotEmpty(t, list)
	for _, node := range list {
		require.NotEqual(t, "", node.Name)
		require.NotEqual(t, "", node.Owner)
	}
}

func (s *Suite) TestMultisetAttributes(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	p := tmpPath()

	_, err := yc.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
		Recursive: true,
	})
	require.NoError(t, err)

	attrs := map[string]any{
		"first_attr":  "some_value",
		"second_attr": 42,
	}
	err = yc.MultisetAttributes(ctx, p.Attrs(), attrs, nil)
	require.NoError(t, err)

	cypressAttrs := struct {
		FirstAttr  string `yson:"first_attr"`
		SecondAttr int    `yson:"second_attr"`
	}{}
	err = yc.GetNode(ctx, p.Attrs(), &cypressAttrs, nil)
	require.NoError(t, err)

	require.Equal(t, attrs["first_attr"], cypressAttrs.FirstAttr)
	require.Equal(t, attrs["second_attr"], cypressAttrs.SecondAttr)
}

func (s *Suite) TestCopyNode(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	p := tmpPath()
	dst := tmpPath()

	_, err := yc.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
		Attributes: map[string]any{"foo": "bar"},
		Recursive:  true,
	})
	require.NoError(t, err)

	_, err = yc.CopyNode(ctx, p, dst, nil)
	require.NoError(t, err)

	ok, err := yc.NodeExists(ctx, dst.Attr("foo"), nil)
	require.NoError(t, err)
	require.True(t, ok)
}

func (s *Suite) TestMoveNode(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	p := tmpPath()
	dst := tmpPath()

	_, err := yc.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
		Attributes: map[string]any{"foo": "bar"},
		Recursive:  true,
	})
	require.NoError(t, err)

	_, err = yc.MoveNode(ctx, p, dst, nil)
	require.NoError(t, err)

	ok, err := yc.NodeExists(ctx, dst.Attr("foo"), nil)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = yc.NodeExists(ctx, p, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func (s *Suite) TestLinkNode(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	p := tmpPath()
	link := tmpPath()

	_, err := yc.CreateNode(ctx, p, yt.NodeString, &yt.CreateNodeOptions{
		Recursive: true,
	})
	require.NoError(t, err)

	_, err = yc.LinkNode(ctx, p, link, nil)
	require.NoError(t, err)

	var typ yt.NodeType
	err = yc.GetNode(ctx, link.SuppressSymlink().Attr("type"), &typ, nil)
	require.NoError(t, err)
	require.Equal(t, yt.NodeLink, typ)
}

func (s *Suite) TestCreateObject(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	account := guid.New().String()
	_, err := yc.CreateObject(ctx, yt.NodeAccount, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": account,
		},
	})
	require.NoError(t, err)

	p := ypath.Path("//sys/accounts").Child(account)
	ok, err := yc.NodeExists(ctx, p, nil)
	require.NoError(t, err)
	require.True(t, ok)
}

func (s *Suite) TestBinaryPath(t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(s.Ctx, time.Second*30)
	defer cancel()

	var value any
	err := yc.GetNode(ctx, ypath.Path("/привет"), &value, nil)
	require.Error(t, err)

	require.Contains(t, err.Error(), "привет")
}
