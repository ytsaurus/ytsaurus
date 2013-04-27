#include "stdafx.h"
#include "table_node.h"
#include "table_node_proxy.h"
#include "private.h"

#include <ytlib/chunk_client/schema.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NTableServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NYTree;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NTableClient;
using namespace NTransactionServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChunkList_(nullptr)
    , UpdateMode_(NChunkClient::EUpdateMode::None)
    , ReplicationFactor_(0)
    , Codec_(NCompression::ECodec::Lz4)
{ }

int TTableNode::GetOwningReplicationFactor() const
{
    return GetTrunkNode()->GetReplicationFactor();
}

EObjectType TTableNode::GetObjectType() const
{
    return EObjectType::Table;
}

void TTableNode::Save(const NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    auto* output = context.GetOutput();
    SaveObjectRef(context, ChunkList_);
    ::Save(output, UpdateMode_);
    ::Save(output, ReplicationFactor_);
}

void TTableNode::Load(const NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    auto* input = context.GetInput();
    LoadObjectRef(context, ChunkList_);
    ::Load(input, UpdateMode_);
    ::Load(input, ReplicationFactor_);

    // COMPAT(psushin)
    if (context.GetVersion() < 10) {
        ::Load(input, Codec_);
    }
}

TClusterResources TTableNode::GetResourceUsage() const
{
    const auto* chunkList = GetUsageChunkList();
    i64 diskSpace = chunkList ? chunkList->Statistics().DiskSpace * GetOwningReplicationFactor() : 0;
    return TClusterResources(diskSpace, 1);
}

TTableNode* TTableNode::GetTrunkNode() const
{
    return static_cast<TTableNode*>(TrunkNode_);
}

const TChunkList* TTableNode::GetUsageChunkList() const
{
    switch (UpdateMode_) {
        case NChunkClient::EUpdateMode::None:
            if (Transaction_) {
                return nullptr;;
            }
            return ChunkList_;

        case NChunkClient::EUpdateMode::Append: {
            const auto& children = ChunkList_->Children();
            YCHECK(children.size() == 2);
            return children[1]->AsChunkList();
        }

        case NChunkClient::EUpdateMode::Overwrite:
            return ChunkList_;

        default:
            YUNREACHABLE();
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

    virtual void SetDefaultAttributes(IAttributeDictionary* attributes) override
    {
        if (!attributes->Contains("channels")) {
            attributes->SetYson("channels", TYsonString("[]"));
        }
        if (!attributes->Contains("replication_factor")) {
            attributes->Set("replication_factor", 3);
        }
        if (!attributes->Contains("compression_codec")) {
            NCompression::ECodec codecId = NCompression::ECodec::Lz4;
            attributes->SetYson(
                "compression_codec",
                ConvertToYsonString(codecId));
        }
    }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::Table;
    }

    virtual ENodeType GetNodeType() override
    {
        return ENodeType::Entity;
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TTableNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateTableNodeProxy(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

    virtual TAutoPtr<TTableNode> DoCreate(
        TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response) override
    {
        YCHECK(request);
        UNUSED(transaction);
        UNUSED(response);

        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();

        auto node = TBase::DoCreate(transaction, request, response);

        // Create an empty chunk list and reference it from the node.
        auto* chunkList = chunkManager->CreateChunkList();
        node->SetChunkList(chunkList);
        YCHECK(chunkList->OwningNodes().insert(~node).second);
        objectManager->RefObject(chunkList);

        return node;
    }

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

        branchedNode->SetReplicationFactor(originatingNode->GetReplicationFactor());

        LOG_DEBUG_UNLESS(IsRecovery(), "Table node branched (BranchedNodeId: %s, ChunkListId: %s, ReplicationFactor: %d)",
            ~branchedNode->GetId().ToString(),
            ~originatingNode->GetChunkList()->GetId().ToString(),
            originatingNode->GetReplicationFactor());
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
            "Table node merged (OriginatingNodeId: %s, OriginatingChunkListId: %s, OriginatingUpdateMode: %s, OriginatingReplicationFactor: %d, "
            "BranchedNodeId: %s, BranchedChunkListId: %s, BranchedUpdateMode: %s, BranchedReplicationFactor: %d, "
            "NewOriginatingChunkListId: %s, NewOriginatingUpdateMode: %s)",
            ~originatingNode->GetId().ToString(),
            ~originatingChunkListId.ToString(),
            ~originatingUpdateMode.ToString(),
            originatingNode->GetReplicationFactor(),
            ~branchedNode->GetId().ToString(),
            ~branchedChunkListId.ToString(),
            ~branchedUpdateMode.ToString(),
            branchedNode->GetReplicationFactor(),
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
        if (branchedMode == NChunkClient::EUpdateMode::None) {
            objectManager->UnrefObject(branchedChunkList);
            return;
        }

        bool isTopmostCommit = !originatingNode->GetTransaction();
        bool isRFUpdateNeeded =
            isTopmostCommit &&
            originatingNode->GetReplicationFactor() != branchedNode->GetReplicationFactor() &&
            metaStateManager->IsLeader();

        if (branchedMode == NChunkClient::EUpdateMode::Overwrite) {
            YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
            YCHECK(branchedChunkList->OwningNodes().insert(originatingNode).second);
            originatingNode->SetChunkList(branchedChunkList);

            if (isRFUpdateNeeded) {
                chunkManager->ScheduleRFUpdate(branchedChunkList);
            }

            objectManager->UnrefObject(originatingChunkList);
        } else if ((originatingMode == NChunkClient::EUpdateMode::None || originatingMode == NChunkClient::EUpdateMode::Overwrite) &&
                   branchedMode == NChunkClient::EUpdateMode::Append)
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

            if (isRFUpdateNeeded) {
                chunkManager->ScheduleRFUpdate(deltaRef);
            }

            objectManager->UnrefObject(originatingChunkList);
            objectManager->UnrefObject(branchedChunkList);
        } else if (originatingMode == NChunkClient::EUpdateMode::Append &&
                   branchedMode == NChunkClient::EUpdateMode::Append)
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

            // Not a topmost commit -- no RF update here.
            YCHECK(!isTopmostCommit);

            objectManager->UnrefObject(originatingChunkList);
            objectManager->UnrefObject(branchedChunkList);

        } else {
            YUNREACHABLE();
        }

        if (isTopmostCommit) {
            // Originating mode must remain None.
            // Rebalance when the topmost transaction commits.
            chunkManager->RebalanceChunkTree(originatingNode->GetChunkList());
        } else {
            // Set proper originating mode.
            originatingNode->SetUpdateMode(
                originatingMode == NChunkClient::EUpdateMode::Overwrite || branchedMode == NChunkClient::EUpdateMode::Overwrite
                ? NChunkClient::EUpdateMode::Overwrite
                : NChunkClient::EUpdateMode::Append);
        }
    }

    virtual void DoClone(
        TTableNode* sourceNode,
        TTableNode* clonedNode,
        const TCloneContext& context) override
    {
        TBase::DoClone(sourceNode, clonedNode, context);

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

