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
        auto chunkManager = Bootstrap->GetChunkManager();
        auto cypressManager = Bootstrap->GetCypressManager();
        auto objectManager = Bootstrap->GetObjectManager();

        TAutoPtr<TTableNode> node(new TTableNode(nodeId));

        // Create an empty chunk list and reference it from the node.
        auto& chunkList = chunkManager->CreateChunkList();
        auto chunkListId = chunkList.GetId();
        node->SetChunkListId(chunkListId);
        objectManager->RefObject(chunkListId);
        cypressManager->RegisterNode(transactionId, node.Release());

        auto proxy = cypressManager->GetVersionedNodeProxy(nodeId, NullTransactionId);
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

    virtual void DoBranch(const TTableNode& originatingNode, TTableNode& branchedNode)
    {
        // branchedNode is a copy of originatingNode.
        
        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();

        // Create composite chunk list and place it in the root of branchedNode.
        auto& branchedChunkList = chunkManager->CreateChunkList();
        auto branchedChunkListId = branchedChunkList.GetId();
        branchedNode.SetChunkListId(branchedChunkListId);
        objectManager->RefObject(branchedChunkListId);

        // Make the original chunk list a child of the composite one.
        yvector<TChunkTreeId> childrenIds;
        childrenIds.push_back(originatingNode.GetChunkListId());
        chunkManager->AttachToChunkList(branchedChunkList, childrenIds);
    }

    // TODO(babenko): this needs much improvement
    virtual void DoMerge(TTableNode& originatingNode, TTableNode& branchedNode)
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();

        // Obtain the chunk list of branchedNode.
        auto branchedChunkListId = branchedNode.GetChunkListId();
        auto& branchedChunkList = chunkManager->GetChunkList(branchedChunkListId);
        YASSERT(branchedChunkList.GetObjectRefCounter() == 1);

        // Replace the first child of the branched chunk list with the current chunk list of originatingNode.
        YASSERT(branchedChunkList.ChildrenIds().size() >= 1);
        auto oldFirstChildId = branchedChunkList.ChildrenIds()[0];
        auto newFirstChildId = originatingNode.GetChunkListId();
        branchedChunkList.ChildrenIds()[0] = newFirstChildId;
        objectManager->RefObject(newFirstChildId);
        objectManager->UnrefObject(oldFirstChildId);

        // Replace the chunk list of originatingNode.
        originatingNode.SetChunkListId(branchedChunkListId);
        objectManager->UnrefObject(newFirstChildId);
    }

};

INodeTypeHandler::TPtr CreateTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

