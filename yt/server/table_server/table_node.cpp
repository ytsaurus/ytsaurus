#include "stdafx.h"
#include "table_node.h"
#include "table_node_proxy.h"
#include "common.h"

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialization_context.h>
#include <ytlib/table_client/schema.h>

namespace NYT {
namespace NTableServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NYTree;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NTableClient;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChunkList_(NULL)
    , UpdateMode_(ETableUpdateMode::None)
    , ReplicationFactor_(0)
{ }

int TTableNode::GetOwningReplicationFactor() const 
{
    auto* trunkNode = TrunkNode_ == this ? this : dynamic_cast<TTableNode*>(TrunkNode_);
    YCHECK(trunkNode);
    return trunkNode->GetReplicationFactor();
}

EObjectType TTableNode::GetObjectType() const
{
    return EObjectType::Table;
}

void TTableNode::Save(const NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    auto* output = context.GetOutput();
    SaveObjectRef(output, ChunkList_);
    ::Save(output, UpdateMode_);
    ::Save(output, ReplicationFactor_);
}

void TTableNode::Load(const NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    auto* input = context.GetInput();
    LoadObjectRef(input, ChunkList_, context);
    ::Load(input, UpdateMode_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 2) {
        ::Load(input, ReplicationFactor_);
    } else {
        ReplicationFactor_ = 3;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTableNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TTableNode>
{
public:
    typedef TCypressNodeTypeHandlerBase<TTableNode> TBase;

    explicit TTableNodeTypeHandler(TBootstrap* bootstrap)
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

    virtual TAutoPtr<ICypressNode> Create(
        TTransaction* transaction,
        IAttributeDictionary* attributes,
        TReqCreate* request,
        TRspCreate* response) override
    {
        YCHECK(request);
        UNUSED(transaction);
        UNUSED(response);

        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();

        // Adjust attributes:
        // - channels
        if (!attributes->Contains("channels")) {
            attributes->SetYson("channels", TYsonString("[]"));
        }
        // - replication_factor
        int replicationFactor = attributes->Get<int>("replication_factor", 3);
        attributes->Remove("replication_factor");

        auto node = TBase::DoCreate(transaction, attributes, request, response);

        node->SetReplicationFactor(replicationFactor);

        // Create an empty chunk list and reference it from the node.
        auto* chunkList = chunkManager->CreateChunkList();
        node->SetChunkList(chunkList);
        YCHECK(chunkList->OwningNodes().insert(~node).second);
        objectManager->RefObject(chunkList);

        // TODO(babenko): Release is needed due to cast to ICypressNode.
        return node.Release();
    }

    virtual ICypressNodeProxyPtr GetProxy(
        ICypressNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TTableNodeProxy>(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

protected:
    virtual void DoDestroy(TTableNode* node) override
    {
        TBase::DoDestroy(node);

        auto objectManager = Bootstrap->GetObjectManager();

        auto* chunkList = node->GetChunkList();
        YCHECK(chunkList->OwningNodes().erase(node) == 1);
        objectManager->UnrefObject(chunkList);
    }

    virtual void DoBranch(const TTableNode* originatingNode, TTableNode* branchedNode) override
    {
    	TBase::DoBranch(originatingNode, branchedNode);

        auto objectManager = Bootstrap->GetObjectManager();

        auto* chunkList = originatingNode->GetChunkList();

        branchedNode->SetChunkList(chunkList);
        objectManager->RefObject(branchedNode->GetChunkList());
        YCHECK(branchedNode->GetChunkList()->OwningNodes().insert(branchedNode).second);
        
        LOG_DEBUG_UNLESS(IsRecovery(), "Table node branched (BranchedNodeId: %s, ChunkListId: %s)",
            ~branchedNode->GetId().ToString(),
            ~originatingNode->GetChunkList()->GetId().ToString());
    }

    virtual void DoMerge(TTableNode* originatingNode, TTableNode* branchedNode) override
    {
    	TBase::DoMerge(originatingNode, branchedNode);

        auto originatingChunkListId = originatingNode->GetChunkList()->GetId();
        auto branchedChunkListId = branchedNode->GetChunkList()->GetId();

        auto originatingUpdateMode = originatingNode->GetUpdateMode();
        auto branchedUpdateMode = branchedNode->GetUpdateMode();

        MergeChunkLists(originatingNode, branchedNode);

        LOG_DEBUG_UNLESS(IsRecovery(),
            "Table node merged (OriginatingNodeId: %s, OriginatingChunkListId: %s, OriginatingUpdateMode: %s, "
            "BranchedNodeId: %s, BranchedChunkListId: %s, BranchedUpdateMode: %s, "
            "NewOriginatingChunkListId: %s, NewOriginatingUpdateMode: %s)",
            ~originatingNode->GetId().ToString(),
            ~originatingChunkListId.ToString(),
            ~originatingUpdateMode.ToString(),
            ~branchedNode->GetId().ToString(),
            ~branchedChunkListId.ToString(),
            ~branchedUpdateMode.ToString(),
            ~originatingNode->GetChunkList()->GetId().ToString(),
            ~originatingNode->GetUpdateMode().ToString());
    }

    void MergeChunkLists(TTableNode* originatingNode, TTableNode* branchedNode)
    {
        auto metaStateManager = Bootstrap->GetMetaStateFacade()->GetManager();
        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();

        auto* originatingChunkList = originatingNode->GetChunkList();
        auto* branchedChunkList = branchedNode->GetChunkList();

        auto originatingMode = originatingNode->GetUpdateMode();
        auto branchedMode = branchedNode->GetUpdateMode();

        YCHECK(branchedChunkList->OwningNodes().erase(branchedNode) == 1);

        // Check if we have anything to do at all.
        if (branchedMode == ETableUpdateMode::None) {
            objectManager->UnrefObject(branchedChunkList);
            return;
        }

        if (branchedMode == ETableUpdateMode::Overwrite) {
            YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
            YCHECK(branchedChunkList->OwningNodes().insert(originatingNode).second);
            originatingNode->SetChunkList(branchedChunkList);

            objectManager->UnrefObject(originatingChunkList);
        } else if ((originatingMode == ETableUpdateMode::None || originatingMode == ETableUpdateMode::Overwrite) &&
                   branchedMode == ETableUpdateMode::Append)
        {
            YCHECK(branchedChunkList->Children().size() == 2);
            auto deltaRef = branchedChunkList->Children()[1];

            auto* newOriginatingChunkList = chunkManager->CreateChunkList();
            newOriginatingChunkList->SortedBy() = branchedChunkList->SortedBy();

            YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
            YCHECK(newOriginatingChunkList->OwningNodes().insert(originatingNode).second);
            originatingNode->SetChunkList(newOriginatingChunkList);
            objectManager->RefObject(newOriginatingChunkList);
         
            chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList);
            chunkManager->AttachToChunkList(newOriginatingChunkList, deltaRef);

            objectManager->UnrefObject(originatingChunkList);
            objectManager->UnrefObject(branchedChunkList);
        } else if (originatingMode == ETableUpdateMode::Append &&
                   branchedMode == ETableUpdateMode::Append)
        {
            YCHECK(originatingChunkList->Children().size() == 2);
            YCHECK(branchedChunkList->Children().size() == 2);

            auto* newOriginatingChunkList = chunkManager->CreateChunkList();
            newOriginatingChunkList->SortedBy() = branchedChunkList->SortedBy();

            YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
            YCHECK(newOriginatingChunkList->OwningNodes().insert(originatingNode).second);
            originatingNode->SetChunkList(newOriginatingChunkList);
            objectManager->RefObject(newOriginatingChunkList);

            chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList->Children()[0]);

            auto* newDeltaChunkList = chunkManager->CreateChunkList();
            chunkManager->AttachToChunkList(newOriginatingChunkList, newDeltaChunkList);

            chunkManager->AttachToChunkList(newDeltaChunkList, originatingChunkList->Children()[1]);
            chunkManager->AttachToChunkList(newDeltaChunkList, branchedChunkList->Children()[1]);

            objectManager->UnrefObject(originatingChunkList);
            objectManager->UnrefObject(branchedChunkList);

        } else {
            YUNREACHABLE();
        }

        if (originatingNode->GetId().IsBranched()) {
            // Set proper originating mode.
            originatingNode->SetUpdateMode(
                originatingMode == ETableUpdateMode::Overwrite || branchedMode == ETableUpdateMode::Overwrite
                ? ETableUpdateMode::Overwrite
                : ETableUpdateMode::Append);
        } else {
            // Originating mode must always remains None.
            // Rebalance and schedule RF update when the topmost transaction commits.
            auto* newOriginatingChunkList = originatingNode->GetChunkList();

            chunkManager->RebalanceChunkTree(newOriginatingChunkList);

            if (metaStateManager->IsLeader()) {
                chunkManager->ScheduleRFUpdate(newOriginatingChunkList);
            }
        }
    }

    virtual void DoClone(
        TTableNode* sourceNode,
        TTableNode* clonedNode,
        TTransaction* transaction) override
    {
        TBase::DoClone(sourceNode, clonedNode, transaction);

        auto objectManager = Bootstrap->GetObjectManager();

        auto* chunkList = sourceNode->GetChunkList();
        YCHECK(!clonedNode->GetChunkList());
        clonedNode->SetChunkList(chunkList);
        clonedNode->SetReplicationFactor(sourceNode->GetReplicationFactor());
        objectManager->RefObject(chunkList);
        YCHECK(chunkList->OwningNodes().insert(clonedNode).second);
    }
};

INodeTypeHandlerPtr CreateTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

