#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "file_manager.h"

#include "../cypress/node_proxy.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NFileServer {

using namespace NYTree;
using namespace NCypress;
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

void TFileNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    ::Save(output, ChunkListId_);
}

void TFileNode::Load(TInputStream* input)
{
    TCypressNodeBase::Load(input);
    ::Load(input, ChunkListId_);
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
    // NB: No smartpointer for this here.
    RegisterGetter("size", FromMethod(&TThis::GetSize, this));
    RegisterGetter("chunk_list_id", FromMethod(&TThis::GetChunkListId));
    RegisterGetter("chunk_id", FromMethod(&TThis::GetChunkId, this));
}

void TFileNodeTypeHandler::GetSize(const TGetAttributeParam& param)
{
    const auto* chunk = GetChunk(*param.Node);
    i64 size = chunk == NULL ? -1 : chunk->GetSize();
    BuildYsonFluently(param.Consumer)
        .Scalar(size);
}

void TFileNodeTypeHandler::GetChunkListId(const TGetAttributeParam& param)
{
    BuildYsonFluently(param.Consumer)
        .Scalar(param.Node->GetChunkListId().ToString());
}

void TFileNodeTypeHandler::GetChunkId(const TGetAttributeParam& param)
{
    const auto* chunk = GetChunk(*param.Node);
    auto chunkId = chunk == NULL ? TChunkId() : chunk->GetId();
    BuildYsonFluently(param.Consumer)
        .Scalar(chunkId.ToString());
}

const NChunkServer::TChunk* TFileNodeTypeHandler::GetChunk(const TFileNode& node)
{
    if (node.GetChunkListId() == NullChunkListId) {
        return NULL;
    }

    const auto& chunkList = ChunkManager->GetChunkList(node.GetChunkListId());
    YASSERT(chunkList.Chunks().ysize() == 1);

    return &ChunkManager->GetChunk(chunkList.Chunks()[0]);
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

ENodeType TFileNodeTypeHandler::GetNodeType()
{
    return ENodeType::Entity;
}

Stroka TFileNodeTypeHandler::GetTypeName()
{
    return FileTypeName;
}

TAutoPtr<ICypressNode> TFileNodeTypeHandler::CreateFromManifest(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    IMapNode::TPtr manifest)
{
    UNUSED(transactionId);
    UNUSED(manifest);

    TAutoPtr<TFileNode> node(new TFileNode(TBranchedNodeId(nodeId, NullTransactionId)));
    return node.Release();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

