#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "private.h"

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/serialization_context.h>
#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NFileServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NCypressServer;
using namespace NChunkServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NFileClient;
using namespace NCypressClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& SILENT_UNUSED Logger = FileServerLogger;

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChunkList_(nullptr)
    , UpdateMode_(EFileUpdateMode::None)
    , ReplicationFactor_(0)
{ }

int TFileNode::GetOwningReplicationFactor() const
{
    auto* trunkNode = TrunkNode_ == this ? this : dynamic_cast<TFileNode*>(TrunkNode_);
    YCHECK(trunkNode);
    return trunkNode->GetReplicationFactor();
}

void TFileNode::Save(const NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    auto* output = context.GetOutput();
    SaveObjectRef(context, ChunkList_);
    ::Save(output, UpdateMode_);
    ::Save(output, ReplicationFactor_);
}

void TFileNode::Load(const NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    auto* input = context.GetInput();
    LoadObjectRef(context, ChunkList_);
    if (context.GetVersion() >= 6) {
        ::Load(input, UpdateMode_);
    }
    ::Load(input, ReplicationFactor_);
}

TClusterResources TFileNode::GetResourceUsage() const
{
    const auto* chunkList = GetUsageChunkList();
    i64 diskSpace = chunkList ? chunkList->Statistics().DiskSpace * GetOwningReplicationFactor() : 0;
    return TClusterResources(diskSpace, 1);
}

const TChunkList* TFileNode::GetUsageChunkList() const
{
    if (Transaction_) {
        return nullptr;
    }

    return ChunkList_;
}

////////////////////////////////////////////////////////////////////////////////

class TFileNodeTypeHandler
    : public NCypressServer::TCypressNodeTypeHandlerBase<TFileNode>
{
public:
    typedef TCypressNodeTypeHandlerBase<TFileNode> TBase;

    explicit TFileNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual void SetDefaultAttributes(IAttributeDictionary* attributes) override
    {
        if (!attributes->Contains("replication_factor")) {
            attributes->Set("replication_factor", 3);
        }
    }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::File;
    }

    virtual ENodeType GetNodeType() override
    {
        return ENodeType::Entity;
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TFileNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateFileNodeProxy(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

    virtual TAutoPtr<TFileNode> DoCreate(
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response) override
    {
        YCHECK(request);
        UNUSED(transaction);
        UNUSED(response);

        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();

        auto node = TBase::DoCreate(transaction, request, response);

        TChunk* chunk = nullptr;

        if (request->HasExtension(NFileClient::NProto::TReqCreateFileExt::create_file)) {
            const auto& requestExt = request->GetExtension(NFileClient::NProto::TReqCreateFileExt::create_file);
            auto chunkId = FromProto<TChunkId>(requestExt.chunk_id());

            chunk = chunkManager->FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                THROW_ERROR_EXCEPTION("No such chunk: %s", ~ToString(chunkId));
            }
            if (!chunk->IsConfirmed()) {
                THROW_ERROR_EXCEPTION("File chunk is not confirmed: %s", ~ToString(chunkId));
            }
        }

        auto* chunkList = chunkManager->CreateChunkList();
        node->SetChunkList(chunkList);
        YCHECK(chunkList->OwningNodes().insert(~node).second);
        objectManager->RefObject(chunkList);

        if (chunk) {
            chunkManager->AttachToChunkList(chunkList, chunk);
        }

        return node;
    }

    virtual void DoDestroy(TFileNode* node) override
    {
        TBase::DoDestroy(node);

        auto* chunkList = node->GetChunkList();
        YCHECK(chunkList->OwningNodes().erase(node) == 1);
        Bootstrap->GetObjectManager()->UnrefObject(chunkList);
    }

    virtual void DoBranch(const TFileNode* originatingNode, TFileNode* branchedNode) override
    {
        TBase::DoBranch(originatingNode, branchedNode);

        auto objectManager = Bootstrap->GetObjectManager();

        auto* chunkList = originatingNode->GetChunkList();
        branchedNode->SetChunkList(chunkList);
        objectManager->RefObject(chunkList);
        YCHECK(chunkList->OwningNodes().insert(branchedNode).second);

        branchedNode->SetReplicationFactor(originatingNode->GetReplicationFactor());

        LOG_DEBUG_UNLESS(IsRecovery(), "File node branched (BranchedNodeId: %s, ChunkListId: %s, ReplicationFactor: %d)",
            ~ToString(branchedNode->GetId()),
            ~ToString(originatingNode->GetChunkList()->GetId()),
            originatingNode->GetReplicationFactor());
    }

    virtual void DoMerge(TFileNode* originatingNode, TFileNode* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        auto* originatingChunkList = originatingNode->GetChunkList();
        auto originatingChunkListId = originatingChunkList->GetId();
        auto originatingUpdateMode = originatingNode->GetUpdateMode();

        auto* branchedChunkList = branchedNode->GetChunkList();
        auto branchedChunkListId = branchedChunkList->GetId();
        auto branchedUpdateMode = branchedNode->GetUpdateMode();

        MergeChunkLists(originatingNode, branchedNode);

        LOG_DEBUG_UNLESS(IsRecovery(),
            "File node merged (OriginatingNodeId: %s, OriginatingChunkListId: %s, OriginatingUpdateMode: %s, OriginatingReplicationFactor: %d, "
            "BranchedNodeId: %s, BranchedChunkListId: %s, BranchedUpdateMode: %s, BranchedReplicationFactor: %d, "
            "NewOriginatingChunkListId: %s, NewOriginatingUpdateMode: %s)",
            ~ToString(originatingNode->GetId()),
            ~ToString(originatingChunkListId),
            ~originatingUpdateMode.ToString(),
            originatingNode->GetReplicationFactor(),
            ~ToString(branchedNode->GetId()),
            ~ToString(branchedChunkListId),
            ~branchedUpdateMode.ToString(),
            branchedNode->GetReplicationFactor(),
            ~ToString(originatingNode->GetChunkList()->GetId()),
            ~originatingNode->GetUpdateMode().ToString());
    }

    void MergeChunkLists(TFileNode* originatingNode, TFileNode* branchedNode)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto chunkManager = Bootstrap->GetChunkManager();
        auto metaStateManager = Bootstrap->GetMetaStateFacade()->GetManager();

        auto* originatingChunkList = originatingNode->GetChunkList();

        auto* branchedChunkList = branchedNode->GetChunkList();
        auto branchedMode = branchedNode->GetUpdateMode();

        YCHECK(branchedChunkList->OwningNodes().erase(branchedNode) == 1);

        // Check if we have anything to do at all.
        if (branchedMode == EFileUpdateMode::None) {
            objectManager->UnrefObject(branchedChunkList);
            return;
        }

        bool isTopmostCommit = !originatingNode->GetTransaction();
        bool isRFUpdateNeeded =
            isTopmostCommit &&
            originatingNode->GetReplicationFactor() != branchedNode->GetReplicationFactor() &&
            metaStateManager->IsLeader();

        YCHECK(branchedMode == EFileUpdateMode::Overwrite);
        YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
        YCHECK(branchedChunkList->OwningNodes().insert(originatingNode).second);
        originatingNode->SetChunkList(branchedChunkList);
        objectManager->UnrefObject(originatingChunkList);

        if (isRFUpdateNeeded) {
            chunkManager->ScheduleRFUpdate(branchedChunkList);
        }

        if (!isTopmostCommit) {
            originatingNode->SetUpdateMode(EFileUpdateMode::Overwrite);
        }
    }

    virtual void DoClone(
        TFileNode* sourceNode,
        TFileNode* clonedNode,
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

INodeTypeHandlerPtr CreateFileTypeHandler(NCellMaster::TBootstrap* bootstrap)
{
    return New<TFileNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

