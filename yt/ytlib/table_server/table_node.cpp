#include "stdafx.h"
#include "table_node.h"
#include "table_node_proxy.h"

#include "../cypress/node_proxy.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NTableServer {

using namespace NCypress;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TBranchedNodeId& id, ERuntimeNodeType runtimeType)
    : TCypressNodeBase(id, runtimeType)
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

class TTableNodeTypeHandler
    : public NCypress::TCypressNodeTypeHandlerBase<TTableNode>
{
public:
    TTableNodeTypeHandler(
        NCypress::TCypressManager* cypressManager,
        NChunkServer::TChunkManager* chunkManager)
        : TCypressNodeTypeHandlerBase<TTableNode>(cypressManager)
        , ChunkManager(chunkManager)
    {
        RegisterGetter("chunk_list_ids", FromMethod(&TThis::GetChunkListIds));
    }

    NCypress::ERuntimeNodeType GetRuntimeType()
    {
        return ERuntimeNodeType::Table;
    }

    NYTree::ENodeType GetNodeType()
    {
        return ENodeType::Entity;
    }

    Stroka GetTypeName()
    {
        return TableTypeName;
    }

    virtual TAutoPtr<NCypress::ICypressNode> CreateFromManifest(
        const NCypress::TNodeId& nodeId,
        const NTransactionServer::TTransactionId& transactionId,
        NYTree::INode* manifest)
    {
        UNUSED(transactionId);
        UNUSED(manifest);

        return new TTableNode(
            TBranchedNodeId(nodeId, NullTransactionId),
            GetRuntimeType());
    }

    virtual TIntrusivePtr<NCypress::ICypressNodeProxy> GetProxy(
        const NCypress::ICypressNode& node,
        const NTransactionServer::TTransactionId& transactionId)
    {
        return New<TTableNodeProxy>(
            this,
            ~CypressManager,
            ~ChunkManager,
            transactionId,
            node.GetId().NodeId);
    }

protected:
    virtual void DoDestroy(TTableNode& node)
    {
        FOREACH(const auto& chunkListId, node.ChunkListIds()) {
            ChunkManager->UnrefChunkList(chunkListId);
        }
    }

    virtual void DoBranch(
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

    virtual void DoMerge(
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

        // Check if some chunks were added during the transaction.
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

private:
    typedef TTableNodeTypeHandler TThis;

    NChunkServer::TChunkManager::TPtr ChunkManager;

    static void GetChunkListIds(const TGetAttributeParam& param)
    {
        // TODO: use new fluent API
        param.Consumer->OnBeginList();
        FOREACH (const auto& chunkListId, param.Node->ChunkListIds()) {
            param.Consumer->OnListItem();
            param.Consumer->OnStringScalar(chunkListId.ToString(), false);
        }
        param.Consumer->OnEndList(false);
    }
};

INodeTypeHandler::TPtr CreateTableTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    return New<TTableNodeTypeHandler>(
        cypressManager,
        chunkManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

