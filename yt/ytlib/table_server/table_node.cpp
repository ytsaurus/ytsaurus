#include "stdafx.h"
#include "table_node.h"
#include "table_node_proxy.h"
#include "table_manager.h"

#include "../cypress/node_proxy.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NTableServer {

using namespace NCypress;
using namespace NTransaction;
using namespace NYTree;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TBranchedNodeId& id)
    : TCypressNodeBase(id)
{ }

TTableNode::TTableNode(const TBranchedNodeId& id, const TTableNode& other)
    : TCypressNodeBase(id, other)
    , ChunkListIds_(other.ChunkListIds_)
{ }

ERuntimeNodeType TTableNode::GetRuntimeType() const
{
    return ERuntimeNodeType::Table;
}

TAutoPtr<ICypressNode> TTableNode::Clone() const
{
    return new TTableNode(Id, *this);
}

void TTableNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    ::Save(output, ChunkListIds_);
}

void TTableNode::Load(TInputStream* input)
{
    TCypressNodeBase::Load(input);
    ::Load(input, ChunkListIds_);
}

////////////////////////////////////////////////////////////////////////////////

TTableNodeTypeHandler::TTableNodeTypeHandler(
    TCypressManager* cypressManager,
    TTableManager* tableManager,
    TChunkManager* chunkManager)
    : TCypressNodeTypeHandlerBase<TTableNode>(cypressManager)
    , TableManager(tableManager)
    , ChunkManager(chunkManager)
{
}

void TTableNodeTypeHandler::DoDestroy(TTableNode& node)
{
    FOREACH(const auto& chunkListId, node.ChunkListIds()) {
        ChunkManager->UnrefChunkList(chunkListId);
    }
}

void TTableNodeTypeHandler::DoBranch(
    const TTableNode& committedNode,
    TTableNode& branchedNode)
{
    UNUSED(branchedNode);

    FOREACH(const auto& chunkListId, committedNode.ChunkListIds()) {
        ChunkManager->RefChunkList(chunkListId);
    }
}

void TTableNodeTypeHandler::DoMerge(
    TTableNode& committedNode,
    TTableNode& branchedNode)
{
    FOREACH(const auto& chunkListId, committedNode.ChunkListIds()) {
        ChunkManager->UnrefChunkList(chunkListId);
    }

    committedNode.ChunkListIds().swap(branchedNode.ChunkListIds());
}

TIntrusivePtr<ICypressNodeProxy> TTableNodeTypeHandler::GetProxy(
    const ICypressNode& node,
    const TTransactionId& transactionId)
{
    return New<TTableNodeProxy>(
        this,
        ~CypressManager,
        transactionId,
        node.GetId().NodeId);
}

ERuntimeNodeType TTableNodeTypeHandler::GetRuntimeType()
{
    return ERuntimeNodeType::Table;
}

ENodeType TTableNodeTypeHandler::GetNodeType()
{
    return ENodeType::Entity;
}

Stroka TTableNodeTypeHandler::GetTypeName()
{
    return TableTypeName;
}

TAutoPtr<ICypressNode> TTableNodeTypeHandler::CreateFromManifest(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    IMapNode::TPtr manifest)
{
    UNUSED(transactionId);
    UNUSED(manifest);

    TAutoPtr<TTableNode> node(new TTableNode(TBranchedNodeId(nodeId, NullTransactionId)));
    return node.Release();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

