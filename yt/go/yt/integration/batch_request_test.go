package integration

import (
	"context"
	"regexp"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

var (
	builtinAttributeRE = regexp.MustCompile("Builtin attribute")
)

func TestBatchRequest(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "CreateMultipleTables", Test: suite.TestBatchCreateMultipleTables, SkipRPC: true},
		{Name: "RemoveTables", Test: suite.TestBatchRemoveTables, SkipRPC: true},
		{Name: "RemoveWithMutatingOptions", Test: suite.TestBatchRemoveWithMutatingOptions, SkipRPC: true},
		{Name: "CheckExists", Test: suite.TestBatchCheckExists, SkipRPC: true},
		{Name: "MultisetAttributes", Test: suite.TestBatchMultisetAttributes, SkipRPC: true},
		{Name: "CopyTables", Test: suite.TestBatchCopyTables, SkipRPC: true},
		{Name: "GetNode", Test: suite.TestBatchGetNode, SkipRPC: true},
	})
}

func (s *Suite) TestBatchCreateMultipleTables(ctx context.Context, t *testing.T, yc yt.Client) {
	paths := make([]ypath.Path, 0, 5)
	for i := 0; i < 5; i++ {
		paths = append(paths, s.TmpPath())
	}

	batchReq, err := yc.NewBatchRequest()
	require.NoError(t, err)

	var responses []yt.BatchResponse[yt.NodeID]
	for _, path := range paths {
		resp, err := batchReq.CreateNode(ctx, path, yt.NodeTable, nil)
		require.NoError(t, err)
		responses = append(responses, resp)
	}

	err = batchReq.ExecuteBatch(ctx, &yt.ExecuteBatchRequestOptions{BatchPartMaxSize: ptr.T(2)})
	require.NoError(t, err)

	err = batchReq.ExecuteBatch(ctx, nil)
	require.Error(t, err)

	var ids []yt.NodeID
	for _, resp := range responses {
		require.NoError(t, resp.Error())
		res1 := resp.Result()
		res2 := resp.Result()
		require.Equal(t, res1, res2)
		ids = append(ids, res1)
	}

	for i, path := range paths {
		ok, err := s.YT.NodeExists(ctx, path, nil)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = s.YT.NodeExists(ctx, ids[i].YPath(), nil)
		require.NoError(t, err)
		require.True(t, ok)

		var id yt.NodeID
		require.NoError(t, s.YT.GetNode(ctx, path.Attr("id"), &id, nil))
		require.Equal(t, ids[i], id)
	}
}

func (s *Suite) TestBatchRemoveTables(ctx context.Context, t *testing.T, yc yt.Client) {
	nonExistingPath := s.TmpPath()
	path := s.TmpPath()

	_, err := s.YT.CreateNode(ctx, path, yt.NodeTable, nil)
	require.NoError(t, err)

	batchReq, err := yc.NewBatchRequest()
	require.NoError(t, err)

	nonExistingResp, err := batchReq.RemoveNode(ctx, nonExistingPath, nil)
	require.NoError(t, err)

	existingResp, err := batchReq.RemoveNode(ctx, path, nil)
	require.NoError(t, err)

	err = batchReq.ExecuteBatch(ctx, nil)
	require.NoError(t, err)

	require.True(t, yterrors.ContainsResolveError(nonExistingResp.Error()))
	require.NoError(t, existingResp.Error())
	require.NoError(t, existingResp.Error())

	ok, err := s.YT.NodeExists(ctx, path, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func (s *Suite) TestBatchRemoveWithMutatingOptions(ctx context.Context, t *testing.T, yc yt.Client) {
	path := s.TmpPath()
	_, err := s.YT.CreateNode(ctx, path, yt.NodeTable, nil)
	require.NoError(t, err)

	mutationID := yt.MutationID(guid.New())

	batchReq1, err := yc.NewBatchRequest()
	require.NoError(t, err)

	resp1, err := batchReq1.RemoveNode(ctx, path, nil)
	require.NoError(t, err)

	err = batchReq1.ExecuteBatch(ctx, &yt.ExecuteBatchRequestOptions{MutatingOptions: &yt.MutatingOptions{MutationID: mutationID}})
	require.NoError(t, err)

	require.NoError(t, resp1.Error())
	ok, err := s.YT.NodeExists(ctx, path, nil)
	require.NoError(t, err)
	require.False(t, ok)

	batchReq2, err := yc.NewBatchRequest()
	require.NoError(t, err)

	resp2, err := batchReq2.RemoveNode(ctx, path, nil)
	require.NoError(t, err)

	err = batchReq2.ExecuteBatch(ctx, &yt.ExecuteBatchRequestOptions{MutatingOptions: &yt.MutatingOptions{MutationID: mutationID, Retry: true}})
	require.NoError(t, err)

	require.NoError(t, resp2.Error())
	ok, err = s.YT.NodeExists(ctx, path, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func (s *Suite) TestBatchCheckExists(ctx context.Context, t *testing.T, yc yt.Client) {
	nonExistingPath := s.TmpPath()
	path := s.TmpPath()

	_, err := s.YT.CreateNode(ctx, path, yt.NodeTable, nil)
	require.NoError(t, err)

	batchReq, err := yc.NewBatchRequest()
	require.NoError(t, err)

	var wg sync.WaitGroup

	checkNonExistingPathResponse, err := batchReq.NodeExists(ctx, nonExistingPath, nil)
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, checkNonExistingPathResponse.Error())
		require.False(t, checkNonExistingPathResponse.Result())
	}()

	resp, err := batchReq.NodeExists(ctx, path, nil)
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, resp.Error())
		require.True(t, resp.Result())
	}()

	err = batchReq.ExecuteBatch(ctx, nil)
	require.NoError(t, err)

	wg.Wait()
}

func (s *Suite) TestBatchMultisetAttributes(ctx context.Context, t *testing.T, yc yt.Client) {
	path1 := s.TmpPath()
	path2 := s.TmpPath()

	_, err := s.YT.CreateNode(ctx, path1, yt.NodeTable, nil)
	require.NoError(t, err)

	_, err = s.YT.CreateNode(ctx, path2, yt.NodeTable, nil)
	require.NoError(t, err)

	batchReq, err := yc.NewBatchRequest()
	require.NoError(t, err)

	attrs1 := map[string]any{
		"attr1": "value1",
		"attr2": 2,
	}
	resp1, err := batchReq.MultisetAttributes(ctx, path1.Attrs(), attrs1, nil)
	require.NoError(t, err)

	attrs2 := map[string]any{
		"type":      "file",
		"user-attr": 2,
	}
	resp2, err := batchReq.MultisetAttributes(ctx, path2.Attrs(), attrs2, nil)
	require.NoError(t, err)

	err = batchReq.ExecuteBatch(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, resp1.Error())

	var actualAttrs1 map[string]any
	err = s.YT.GetNode(ctx, path1.Attrs(), &actualAttrs1, nil)
	require.NoError(t, err)

	for attrKey, attrValue := range attrs1 {
		value, ok := actualAttrs1[attrKey]
		require.True(t, ok)
		require.EqualValues(t, attrValue, value)
	}

	require.Error(t, resp2.Error())
	require.True(t, yterrors.ContainsMessageRE(resp2.Error(), builtinAttributeRE))
}

func (s *Suite) TestBatchCopyTables(ctx context.Context, t *testing.T, yc yt.Client) {
	path := s.TmpPath()
	pathCopy1 := s.TmpPath()
	pathCopy2 := s.TmpPath()

	_, err := s.YT.CreateNode(ctx, path, yt.NodeTable, nil)
	require.NoError(t, err)

	batchReq, err := yc.NewBatchRequest()
	require.NoError(t, err)

	resp1, err := batchReq.CopyNode(ctx, path, pathCopy1, nil)
	require.NoError(t, err)

	resp2, err := batchReq.CopyNode(ctx, path, pathCopy2, nil)
	require.NoError(t, err)

	err = batchReq.ExecuteBatch(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, resp1.Error())
	require.NoError(t, resp2.Error())

	ok, err := s.YT.NodeExists(ctx, resp1.Result().YPath(), nil)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = s.YT.NodeExists(ctx, resp2.Result().YPath(), nil)
	require.NoError(t, err)
	require.True(t, ok)
}

func (s *Suite) TestBatchGetNode(ctx context.Context, t *testing.T, yc yt.Client) {
	path := s.TmpPath()

	_, err := s.YT.CreateNode(ctx, path, yt.NodeTable, nil)
	require.NoError(t, err)

	attrs := map[string]any{"user-attr1": "value1", "user-attr2": true}
	require.NoError(t, s.YT.MultisetAttributes(ctx, path.Attrs(), attrs, nil))

	batchReq, err := yc.NewBatchRequest()
	require.NoError(t, err)

	var nodeType yt.NodeType
	getNodeTypeResponse, err := batchReq.GetNode(ctx, path.Attr("type"), &nodeType, &yt.GetNodeOptions{})
	require.NoError(t, err)

	var userAttrs map[string]any
	getUserAttrsResponse, err := batchReq.GetNode(ctx, path.Attrs(), &userAttrs, &yt.GetNodeOptions{
		Attributes: []string{"user-attr1", "user-attr2"},
	})
	require.NoError(t, err)

	var nonExistingAttr string
	getNonExistingAttrResponse, err := batchReq.GetNode(ctx, path.Attr("non-existing-attr"), &nonExistingAttr, nil)
	require.NoError(t, err)

	require.Empty(t, nodeType)
	require.Nil(t, userAttrs)
	require.Empty(t, nonExistingAttr)

	err = batchReq.ExecuteBatch(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, getNodeTypeResponse.Error())
	require.Equal(t, yt.NodeTable, nodeType)
	require.NoError(t, getNodeTypeResponse.Error())

	require.NoError(t, getUserAttrsResponse.Error())
	require.Equal(t, attrs, userAttrs)

	require.True(t, yterrors.ContainsResolveError(getNonExistingAttrResponse.Error()))
	require.Empty(t, nonExistingAttr)
	require.True(t, yterrors.ContainsResolveError(getNonExistingAttrResponse.Error()))
}
