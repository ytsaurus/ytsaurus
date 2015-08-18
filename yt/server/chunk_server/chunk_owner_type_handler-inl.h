#ifndef CHUNK_OWNER_TYPE_HANDLER_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_owner_type_handler.h"
#endif

#include "helpers.h"

#include <core/erasure/public.h>

#include <ytlib/chunk_client/data_statistics.h>

#include <server/cypress_server/node.h>
#include <server/cypress_server/node_detail.h>
#include <server/cypress_server/cypress_manager.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/object_server/object_manager.h>

#include <server/cell_master/hydra_facade.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkOwner>
TChunkOwnerTypeHandler<TChunkOwner>::TChunkOwnerTypeHandler(NCellMaster::TBootstrap* bootstrap)
    : TBase(bootstrap)
    , Logger(ChunkServerLogger)
{ }

template <class TChunkOwner>
NYTree::ENodeType TChunkOwnerTypeHandler<TChunkOwner>::GetNodeType()
{
    return NYTree::ENodeType::Entity;
}

template <class TChunkOwner>
NSecurityServer::TClusterResources TChunkOwnerTypeHandler<TChunkOwner>::GetTotalResourceUsage(
    const NCypressServer::TCypressNodeBase* node)
{
    const auto* chunkOwnerNode = static_cast<const TChunkOwner*>(node);
    auto result = TBase::GetTotalResourceUsage(node);
    auto statistics = chunkOwnerNode->ComputeTotalStatistics();
    result += GetDiskUsage(statistics, chunkOwnerNode->GetReplicationFactor());
    return result;
}

template <class TChunkOwner>
NSecurityServer::TClusterResources TChunkOwnerTypeHandler<TChunkOwner>::GetAccountingResourceUsage(
    const NCypressServer::TCypressNodeBase* node)
{
    const auto* chunkOwnerNode = static_cast<const TChunkOwner*>(node);
    auto result = TBase::GetAccountingResourceUsage(node);
    auto statistics = chunkOwnerNode->GetUpdateMode() == NChunkClient::EUpdateMode::Append
        ? chunkOwnerNode->DeltaStatistics()
        : chunkOwnerNode->SnapshotStatistics();
    result += GetDiskUsage(statistics, chunkOwnerNode->GetReplicationFactor());
    return result;
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::InitializeAttributes(NYTree::IAttributeDictionary* attributes)
{
    if (!attributes->Contains("replication_factor")) {
        attributes->Set("replication_factor", NChunkClient::DefaultReplicationFactor);
    }

    if (!attributes->Contains("erasure_codec")) {
        attributes->Set("erasure_codec", NErasure::ECodec::None);
    }
}

template <class TChunkOwner>
std::unique_ptr<TChunkOwner> TChunkOwnerTypeHandler<TChunkOwner>::DoCreate(
    const NCypressServer::TVersionedNodeId& id,
    NObjectClient::TCellTag externalCellTag,
    NTransactionServer::TTransaction* transaction,
    NYTree::IAttributeDictionary* attributes,
    NCypressServer::INodeTypeHandler::TReqCreate* request,
    NCypressServer::INodeTypeHandler::TRspCreate* response)
{
    auto chunkManager = this->Bootstrap_->GetChunkManager();
    auto objectManager = this->Bootstrap_->GetObjectManager();

    auto nodeHolder = TBase::DoCreate(
        id,
        externalCellTag,
        transaction,
        attributes,
        request,
        response);
    auto* node = nodeHolder.get();

    if (!node->IsExternal()) {
        // Create an empty chunk list and reference it from the node.
        auto* chunkList = chunkManager->CreateChunkList();
        node->SetChunkList(chunkList);
        YCHECK(chunkList->OwningNodes().insert(node).second);
        objectManager->RefObject(chunkList);
    }

    return nodeHolder;
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoDestroy(TChunkOwner* node)
{
    TBase::DoDestroy(node);

    auto objectManager = TBase::Bootstrap_->GetObjectManager();

    auto* chunkList = node->GetChunkList();
    if (chunkList) {
        YCHECK(chunkList->OwningNodes().erase(node) == 1);
        objectManager->UnrefObject(chunkList);
    }
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoBranch(
    const TChunkOwner* originatingNode,
    TChunkOwner* branchedNode)
{
    TBase::DoBranch(originatingNode, branchedNode);

    auto* chunkList = originatingNode->GetChunkList();
    auto chunkListId = NObjectServer::GetObjectId(chunkList);

    if (!originatingNode->IsExternal()) {
        auto objectManager = TBase::Bootstrap_->GetObjectManager();

        branchedNode->SetChunkList(chunkList);
        objectManager->RefObject(branchedNode->GetChunkList());
        YCHECK(branchedNode->GetChunkList()->OwningNodes().insert(branchedNode).second);
    }

    branchedNode->SetReplicationFactor(originatingNode->GetReplicationFactor());
    branchedNode->SetVital(originatingNode->GetVital());
    branchedNode->SnapshotStatistics() = originatingNode->ComputeTotalStatistics();

    LOG_DEBUG_UNLESS(
        TBase::IsRecovery(),
        "Chunk owner node branched (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v, ReplicationFactor: %v)",
        originatingNode->GetVersionedId(),
        branchedNode->GetVersionedId(),
        chunkListId,
        originatingNode->GetReplicationFactor());
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoMerge(
    TChunkOwner* originatingNode,
    TChunkOwner* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    bool isExternal = originatingNode->IsExternal();

    auto originatingChunkListId = NObjectServer::GetObjectId(originatingNode->GetChunkList());
    auto branchedChunkListId = NObjectServer::GetObjectId(branchedNode->GetChunkList());

    auto originatingUpdateMode = originatingNode->GetUpdateMode();
    auto branchedUpdateMode = branchedNode->GetUpdateMode();

    auto hydraManager = TBase::Bootstrap_->GetHydraFacade()->GetHydraManager();
    auto chunkManager = TBase::Bootstrap_->GetChunkManager();
    auto objectManager = TBase::Bootstrap_->GetObjectManager();

    auto* originatingChunkList = originatingNode->GetChunkList();
    auto* branchedChunkList = branchedNode->GetChunkList();

    auto originatingMode = originatingNode->GetUpdateMode();
    auto branchedMode = branchedNode->GetUpdateMode();

    if (!isExternal) {
        YCHECK(branchedChunkList->OwningNodes().erase(branchedNode) == 1);
    }

    // Check if we have anything to do at all.
    if (branchedMode == NChunkClient::EUpdateMode::None) {
        if (!isExternal) {
            objectManager->UnrefObject(branchedChunkList);
        }
        return;
    }

    bool isTopmostCommit = !originatingNode->GetTransaction();
    bool hasPropertiesChanged =
        originatingNode->GetReplicationFactor() != branchedNode->GetReplicationFactor() ||
        originatingNode->GetVital() != branchedNode->GetVital();
    bool isPropertiesUpdateNeeded = isTopmostCommit && hasPropertiesChanged && hydraManager->IsLeader();
    auto newOriginatingMode = isTopmostCommit || originatingNode->GetType() == NObjectClient::EObjectType::Journal
        ? NChunkClient::EUpdateMode::None
        : (originatingMode == NChunkClient::EUpdateMode::Overwrite || branchedMode == NChunkClient::EUpdateMode::Overwrite)
            ? NChunkClient::EUpdateMode::Overwrite
            : NChunkClient::EUpdateMode::Append;

    if (branchedMode == NChunkClient::EUpdateMode::Overwrite) {
        if (!isExternal) {
            YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
            YCHECK(branchedChunkList->OwningNodes().insert(originatingNode).second);
            originatingNode->SetChunkList(branchedChunkList);

            if (isPropertiesUpdateNeeded) {
                chunkManager->ScheduleChunkPropertiesUpdate(branchedChunkList);
            }

            objectManager->UnrefObject(originatingChunkList);
        }

        originatingNode->SnapshotStatistics() = branchedNode->SnapshotStatistics();
        originatingNode->DeltaStatistics() = branchedNode->DeltaStatistics();
    } else {
        YCHECK(branchedMode == NChunkClient::EUpdateMode::Append);

        TChunkTree* deltaTree = nullptr;
        TChunkList* newOriginatingChunkList = nullptr;
        if (!isExternal) {
            YCHECK(branchedChunkList->Children().size() == 2);
            deltaTree = branchedChunkList->Children()[1];
            newOriginatingChunkList = chunkManager->CreateChunkList();

            YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
            YCHECK(newOriginatingChunkList->OwningNodes().insert(originatingNode).second);
            originatingNode->SetChunkList(newOriginatingChunkList);
            objectManager->RefObject(newOriginatingChunkList);
        }

        if (originatingMode == NChunkClient::EUpdateMode::Append) {
            YCHECK(!isTopmostCommit); // No need to update properties.
            if (!isExternal) {
                chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList->Children()[0]);
                auto* newDeltaChunkList = chunkManager->CreateChunkList();
                chunkManager->AttachToChunkList(newOriginatingChunkList, newDeltaChunkList);
                chunkManager->AttachToChunkList(newDeltaChunkList, originatingChunkList->Children()[1]);
                chunkManager->AttachToChunkList(newDeltaChunkList, deltaTree);
            }

            originatingNode->DeltaStatistics() += branchedNode->DeltaStatistics();
       } else {
            if (!isExternal) {
                chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList);
                chunkManager->AttachToChunkList(newOriginatingChunkList, deltaTree);

                if (isPropertiesUpdateNeeded) {
                    chunkManager->ScheduleChunkPropertiesUpdate(deltaTree);
                }
            }

            if (newOriginatingMode == NChunkClient::EUpdateMode::Append) {
                originatingNode->DeltaStatistics() += branchedNode->DeltaStatistics();
            } else {
                originatingNode->SnapshotStatistics() += branchedNode->DeltaStatistics();
            }
       }

        if (!isExternal) {
            objectManager->UnrefObject(originatingChunkList);
            objectManager->UnrefObject(branchedChunkList);
        }
    }

    auto* newOriginatingChunkList = originatingNode->GetChunkList();
    auto newOriginatingChunkListId = NObjectServer::GetObjectId(newOriginatingChunkList);

    if (isTopmostCommit && !isExternal) {
        // Rebalance when the topmost transaction commits.
        chunkManager->RebalanceChunkTree(newOriginatingChunkList);
    }

    originatingNode->SetUpdateMode(newOriginatingMode);

    LOG_DEBUG_UNLESS(
        TBase::IsRecovery(),
        "Chunk owner node merged (OriginatingNodeId: %v, OriginatingChunkListId: %v, OriginatingUpdateMode: %v, OriginatingReplicationFactor: %v, "
        "BranchedNodeId: %v, BranchedChunkListId: %v, BranchedUpdateMode: %v, BranchedReplicationFactor: %v, "
        "NewOriginatingChunkListId: %v, NewOriginatingUpdateMode: %v)",
        originatingNode->GetVersionedId(),
        originatingChunkListId,
        originatingUpdateMode,
        originatingNode->GetReplicationFactor(),
        branchedNode->GetVersionedId(),
        branchedChunkListId,
        branchedUpdateMode,
        branchedNode->GetReplicationFactor(),
        newOriginatingChunkListId,
        originatingNode->GetUpdateMode());
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoClone(
    TChunkOwner* sourceNode,
    TChunkOwner* clonedNode,
    NCypressServer::ICypressNodeFactoryPtr factory,
    NCypressServer::ENodeCloneMode mode)
{
    TBase::DoClone(sourceNode, clonedNode, factory, mode);

    if (!sourceNode->IsExternal()) {
        auto objectManager = TBase::Bootstrap_->GetObjectManager();
        auto* chunkList = sourceNode->GetChunkList();
        YCHECK(!clonedNode->GetChunkList());
        clonedNode->SetChunkList(chunkList);
        objectManager->RefObject(chunkList);
        YCHECK(chunkList->OwningNodes().insert(clonedNode).second);
    }

    clonedNode->SetReplicationFactor(sourceNode->GetReplicationFactor());
    clonedNode->SetVital(sourceNode->GetVital());
    clonedNode->SnapshotStatistics() = sourceNode->SnapshotStatistics();
    clonedNode->DeltaStatistics() = sourceNode->DeltaStatistics();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
