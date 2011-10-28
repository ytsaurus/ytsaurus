#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "file_manager.h"

#include "../cypress/node_proxy.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NFileServer {

using namespace NYTree;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TBranchedNodeId& id)
    : TCypressNodeBase(id)
    , ChunkListId_(NullChunkListId)
{ }

TFileNode::TFileNode(const TBranchedNodeId& id, const TFileNode& other)
    : TCypressNodeBase(id, other)
    , ChunkListId_(other.ChunkListId_)
{ }

ERuntimeNodeType TFileNode::GetRuntimeType() const
{
    return ERuntimeNodeType::File;
}

TAutoPtr<ICypressNode> TFileNode::Clone() const
{
    return new TFileNode(Id, *this);
}

////////////////////////////////////////////////////////////////////////////////

TFileNodeTypeHandler::TFileNodeTypeHandler(
    TCypressManager* cypressManager,
    TFileManager* fileManager,
    TChunkManager* chunkManager)
    : TCypressNodeTypeHandlerBase<TFileNode>(cypressManager)
    , FileManager(fileManager)
    , ChunkManager(chunkManager)
{
    RegisterGetter("size", FromMethod(&TThis::GetSize));
    RegisterGetter("chunk_list_id", FromMethod(&TThis::GetChunkListId));
    RegisterGetter("chunk_id", FromMethod(&TThis::GetChunkId));
}

void TFileNodeTypeHandler::GetSize(const TGetAttributeRequest& request)
{
    BuildYsonFluently(request.Consumer)
        // TODO: fixme
        .Scalar(-1);
}

void TFileNodeTypeHandler::GetChunkListId(const TGetAttributeRequest& request)
{
    BuildYsonFluently(request.Consumer)
        .Scalar(request.Node->GetChunkListId().ToString());
}

void TFileNodeTypeHandler::GetChunkId(const TGetAttributeRequest& request)
{
    BuildYsonFluently(request.Consumer)
        // TODO: fixme
        .Scalar(TChunkId().ToString());
}

void TFileNodeTypeHandler::DoDestroy(TFileNode& node)
{
    if (node.GetChunkListId() != NullChunkListId) {
        ChunkManager->UnrefChunkList(node.GetChunkListId());
    }
}

void TFileNodeTypeHandler::DoBranch(
    const TFileNode& committedNode,
    TFileNode& branchedNode)
{
    UNUSED(branchedNode);

    if (committedNode.GetChunkListId() != NullChunkListId) {
        ChunkManager->RefChunkList(committedNode.GetChunkListId());
    }
}

void TFileNodeTypeHandler::DoMerge(
    TFileNode& committedNode,
    TFileNode& branchedNode)
{
    if (committedNode.GetChunkListId() != NullChunkListId) {
        ChunkManager->UnrefChunkList(committedNode.GetChunkListId());
    }

    committedNode.SetChunkListId(branchedNode.GetChunkListId());
}

TIntrusivePtr<ICypressNodeProxy> TFileNodeTypeHandler::GetProxy(
    const ICypressNode& node,
    const TTransactionId& transactionId)
{
    return New<TFileNodeProxy>(
        this,
        ~CypressManager,
        transactionId,
        node.GetId().NodeId);
}

ERuntimeNodeType TFileNodeTypeHandler::GetRuntimeType()
{
    return ERuntimeNodeType::File;
}

Stroka TFileNodeTypeHandler::GetTypeName()
{
    return FileTypeName;
}

TAutoPtr<ICypressNode> TFileNodeTypeHandler::Create(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    IMapNode::TPtr manifest )
{
    UNUSED(transactionId);
    UNUSED(manifest);

    TAutoPtr<TFileNode> node(new TFileNode(TBranchedNodeId(nodeId, NullTransactionId)));
    return node.Release();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

