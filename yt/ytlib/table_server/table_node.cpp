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
    UNUSED(committedNode);

    // branchedNode is a copy of committedNode.

    // Reference shared chunklists from branchedNode.
    FOREACH(const auto& chunkListId, branchedNode.ChunkListIds()) {
        ChunkManager->RefChunkList(chunkListId);
    }

    // Create a new chunk list that will keep all newly added chunks.
    auto& appendChunkList = ChunkManager->CreateChunkList();
    branchedNode.ChunkListIds().push_back(appendChunkList.GetId());

    // Reference this chunklist from branchedNode.
    ChunkManager->RefChunkList(appendChunkList);
}

void TTableNodeTypeHandler::DoMerge(
    TTableNode& committedNode,
    TTableNode& branchedNode)
{
    YASSERT(branchedNode.ChunkListIds().ysize() >= 1);
    YASSERT(branchedNode.ChunkListIds().ysize() >= committedNode.ChunkListIds().ysize() + 1);

    // Drop references to shared chunklists from branchedNode.
    for (auto it = branchedNode.ChunkListIds().begin();
         it != branchedNode.ChunkListIds().end() - 1;
         ++it)
    {
        ChunkManager->UnrefChunkList(*it);
    }

    // Check is some chunks were added during the transaction.
    auto appendChunkListId = branchedNode.ChunkListIds().back();
    auto& appendChunkList = ChunkManager->GetChunkListForUpdate(appendChunkListId);
    if (appendChunkList.ChunkIds().empty()) {
        // No chunks were added, just unref this chunklist.
        // This prevents creation of empty chunklists.
        ChunkManager->UnrefChunkList(appendChunkList);
    } else {
        // Perform the actual merge: add this chunklist to committedNode.
        committedNode.ChunkListIds().push_back(appendChunkListId);
    }
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

