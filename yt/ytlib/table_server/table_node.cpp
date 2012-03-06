#include "stdafx.h"
#include "table_node.h"
#include "table_node_proxy.h"

#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NTableServer {

using namespace NCellMaster;
using namespace NCypress;
using namespace NYTree;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
{ }

TTableNode::TTableNode(const TVersionedNodeId& id, const TTableNode& other)
    : TCypressNodeBase(id, other)
    , ChunkListId_(other.ChunkListId_)
{ }

EObjectType TTableNode::GetObjectType() const
{
    return EObjectType::Table;
}

void TTableNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    ::Save(output, ChunkListId_);
}

void TTableNode::Load(TInputStream* input, const TLoadContext& context)
{
    TCypressNodeBase::Load(input, context);
    ::Load(input, ChunkListId_);
}

////////////////////////////////////////////////////////////////////////////////

class TTableNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TTableNode>
{
public:
    typedef TCypressNodeTypeHandlerBase<TTableNode> TBase;

    TTableNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    EObjectType GetObjectType()
    {
        return EObjectType::Table;
    }

    ENodeType GetNodeType()
    {
        return ENodeType::Entity;
    }

    virtual bool IsLockModeSupported(ELockMode mode)
    {
        return
            mode == ELockMode::Exclusive ||
            mode == ELockMode::Shared ||
            mode == ELockMode::Snapshot;
    }

    virtual void CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        IMapNode* manifest)
    {
        TAutoPtr<TTableNode> node(new TTableNode(nodeId));

        // Create an empty chunk list and reference it from the node.
        auto& chunkList = ChunkManager->CreateChunkList();
        auto chunkListId = chunkList.GetId();
        node->SetChunkListId(chunkListId);
        Bootstrap->GetObjectManager()->RefObject(chunkListId);
        Bootstrap->GetCypressManager()->RegisterNode(transactionId, node.Release());

        auto proxy = Bootstrap->GetCypressManager()->GetVersionedNodeProxy(nodeId, NullTransactionId);
        proxy->Attributes().MergeFrom(manifest);
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(const TVersionedNodeId& id)
    {
        return New<TTableNodeProxy>(
            this,
            Bootstrap,
            id.TransactionId,
            id.ObjectId);
    }

protected:
    virtual void DoDestroy(TTableNode& node)
    {
        Bootstrap->GetObjectManager()->UnrefObject(node.GetChunkListId());
    }

    virtual void DoBranch(
        const TTableNode& originatingNode,
        TTableNode& branchedNode)
    {
        // branchedNode is a copy of originatingNode.
        
        // Create composite chunk list and place it in the root of branchedNode.
        auto& branchedChunkList = Bootstrap->GetChunkManager()->CreateChunkList();
        auto branchedChunkListId = branchedChunkList.GetId();
        branchedNode.SetChunkListId(branchedChunkListId);
        Bootstrap->GetObjectManager()->RefObject(branchedChunkListId);

        // Make the original chunk list a child of the composite one.
        yvector<TChunkTreeId> childrenIds;
        childrenIds.push_back(originatingNode.GetChunkListId());
        Bootstrap->GetChunkManager()->AttachToChunkList(branchedChunkList, childrenIds);
    }

    virtual void DoMerge(
        TTableNode& originatingNode,
        TTableNode& branchedNode)
    {
        // TODO(babenko): this needs much improvement

        // Obtain the chunk list of branchedNode.
        auto branchedChunkListId = branchedNode.GetChunkListId();
        auto& branchedChunkList = Bootstrap->GetChunkManager()->GetChunkList(branchedChunkListId);
        YASSERT(branchedChunkList.GetObjectRefCounter() == 1);

        // Replace the first child of the branched chunk list with the current chunk list of originatingNode.
        YASSERT(branchedChunkList.ChildrenIds().size() >= 1);
        auto oldFirstChildId = branchedChunkList.ChildrenIds()[0];
        auto newFirstChildId = originatingNode.GetChunkListId();
        branchedChunkList.ChildrenIds()[0] = newFirstChildId;
        Bootstrap->GetObjectManager()->RefObject(newFirstChildId);
        Bootstrap->GetObjectManager()->UnrefObject(oldFirstChildId);

        // Replace the chunk list of originatingNode.
        originatingNode.SetChunkListId(branchedChunkListId);
        Bootstrap->GetObjectManager()->UnrefObject(newFirstChildId);
    }

private:
    NChunkServer::TChunkManager::TPtr ChunkManager;

};

INodeTypeHandler::TPtr CreateTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

