#ifndef CHUNK_OWNER_TYPE_HANDLER_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_owner_type_handler.h"
#endif

#include "helpers.h"

#include <core/erasure/public.h>

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
void TChunkOwnerTypeHandler<TChunkOwner>::SetDefaultAttributes(
    NYTree::IAttributeDictionary* attributes,
    NTransactionServer::TTransaction* transaction)
{
    TBase::SetDefaultAttributes(attributes, transaction);

    if (!attributes->Contains("replication_factor")) {
        attributes->Set("replication_factor", NChunkClient::DefaultReplicationFactor);
    }

    if (!attributes->Contains("erasure_codec")) {
        attributes->Set("erasure_codec", NErasure::ECodec::None);
    }
}

template <class TChunkOwner>
NYTree::ENodeType TChunkOwnerTypeHandler<TChunkOwner>::GetNodeType()
{
    return NYTree::ENodeType::Entity;
}

template <class TChunkOwner>
NSecurityServer::TClusterResources TChunkOwnerTypeHandler<TChunkOwner>::GetIncrementalResourceUsage(
    const NCypressServer::TCypressNodeBase* node)
{
    const auto* chunkOwnerNode = static_cast<const TChunkOwner*>(node);
    return
        TBase::GetIncrementalResourceUsage(node) +
        GetDiskUsage(chunkOwnerNode->GetIncrementalChunkList(), chunkOwnerNode->GetReplicationFactor());
}

template <class TChunkOwner>
NSecurityServer::TClusterResources TChunkOwnerTypeHandler<TChunkOwner>::GetTotalResourceUsage(
    const NCypressServer::TCypressNodeBase* node)
{
    const auto* chunkOwnerNode = static_cast<const TChunkOwner*>(node);
    return
        TBase::GetTotalResourceUsage(node) +
        GetDiskUsage(chunkOwnerNode->GetChunkList(), chunkOwnerNode->GetReplicationFactor());
}

template <class TChunkOwner>
std::unique_ptr<TChunkOwner> TChunkOwnerTypeHandler<TChunkOwner>::DoCreate(
    const NCypressServer::TVersionedNodeId& id,
    NObjectClient::TCellTag externalCellTag,
    NYTree::IAttributeDictionary* attributes,
    NCypressServer::INodeTypeHandler::TReqCreate* request,
    NCypressServer::INodeTypeHandler::TRspCreate* response)
{
    auto chunkManager = this->Bootstrap_->GetChunkManager();
    auto objectManager = this->Bootstrap_->GetObjectManager();

    auto nodeHolder = TBase::DoCreate(id, externalCellTag, attributes, request, response);
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

    if (!originatingNode->IsExternal()) {
        auto objectManager = TBase::Bootstrap_->GetObjectManager();

        auto* chunkList = originatingNode->GetChunkList();

        branchedNode->SetChunkList(chunkList);
        objectManager->RefObject(branchedNode->GetChunkList());
        YCHECK(branchedNode->GetChunkList()->OwningNodes().insert(branchedNode).second);
    }

    branchedNode->SetReplicationFactor(originatingNode->GetReplicationFactor());
    branchedNode->SetVital(originatingNode->GetVital());

    LOG_DEBUG_UNLESS(
        TBase::IsRecovery(),
        "Chunk owner node branched (BranchedNodeId: %v, ChunkListId: %v, ReplicationFactor: %v)",
        branchedNode->GetId(),
        GetObjectId(originatingNode->GetChunkList()),
        originatingNode->GetReplicationFactor());
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoMerge(
    TChunkOwner* originatingNode,
    TChunkOwner* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    auto originatingChunkListId = NObjectServer::GetObjectId(originatingNode->GetChunkList());
    auto branchedChunkListId = NObjectServer::GetObjectId(branchedNode->GetChunkList());

    auto originatingUpdateMode = originatingNode->GetUpdateMode();
    auto branchedUpdateMode = branchedNode->GetUpdateMode();

    if (originatingNode->IsExternal()) {
        LOG_DEBUG_UNLESS(
            TBase::IsRecovery(),
            "Chunk owner node merged (OriginatingNodeId: %v, OriginatingUpdateMode: %v, OriginatingReplicationFactor: %v, "
            "BranchedNodeId: %v, BranchedUpdateMode: %v, BranchedReplicationFactor: %v, "
            "NewOriginatingUpdateMode: %v)",
            originatingNode->GetVersionedId(),
            originatingUpdateMode,
            originatingNode->GetReplicationFactor(),
            branchedNode->GetVersionedId(),
            branchedUpdateMode,
            branchedNode->GetReplicationFactor(),
            originatingNode->GetUpdateMode());
    } else {
        MergeChunkLists(originatingNode, branchedNode);

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
            originatingNode->GetChunkList()->GetId(),
            originatingNode->GetUpdateMode());
    }
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::MergeChunkLists(
    TChunkOwner* originatingNode,
    TChunkOwner* branchedNode)
{
    auto hydraManager = TBase::Bootstrap_->GetHydraFacade()->GetHydraManager();
    auto chunkManager = TBase::Bootstrap_->GetChunkManager();
    auto objectManager = TBase::Bootstrap_->GetObjectManager();

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
    bool hasPropertiesChanged =
        originatingNode->GetReplicationFactor() != branchedNode->GetReplicationFactor() ||
        originatingNode->GetVital() != branchedNode->GetVital();
    bool isPropertiesUpdateNeeded = isTopmostCommit && hasPropertiesChanged && hydraManager->IsLeader();

    if (branchedMode == NChunkClient::EUpdateMode::Overwrite) {
        YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
        YCHECK(branchedChunkList->OwningNodes().insert(originatingNode).second);
        originatingNode->SetChunkList(branchedChunkList);

        if (isPropertiesUpdateNeeded) {
            chunkManager->ScheduleChunkPropertiesUpdate(branchedChunkList);
        }

        objectManager->UnrefObject(originatingChunkList);
    } else {
        YCHECK(branchedMode == NChunkClient::EUpdateMode::Append);
        YCHECK(branchedChunkList->Children().size() == 2);
        auto deltaTree = branchedChunkList->Children()[1];

        auto* newOriginatingChunkList = chunkManager->CreateChunkList();

        YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
        YCHECK(newOriginatingChunkList->OwningNodes().insert(originatingNode).second);

        originatingNode->SetChunkList(newOriginatingChunkList);
        objectManager->RefObject(newOriginatingChunkList);

        if (originatingMode == NChunkClient::EUpdateMode::Append) {
            YCHECK(!isTopmostCommit); // No need to update properties.
            chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList->Children()[0]);
            auto* newDeltaChunkList = chunkManager->CreateChunkList();
            chunkManager->AttachToChunkList(newOriginatingChunkList, newDeltaChunkList);
            chunkManager->AttachToChunkList(newDeltaChunkList, originatingChunkList->Children()[1]);
            chunkManager->AttachToChunkList(newDeltaChunkList, deltaTree);
        } else {
            chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList);
            chunkManager->AttachToChunkList(newOriginatingChunkList, deltaTree);

            if (isPropertiesUpdateNeeded) {
                chunkManager->ScheduleChunkPropertiesUpdate(deltaTree);
            }
        }

        objectManager->UnrefObject(originatingChunkList);
        objectManager->UnrefObject(branchedChunkList);
    }

    if (isTopmostCommit) {
        // Originating mode must remain None.
        // Rebalance when the topmost transaction commits.
        chunkManager->RebalanceChunkTree(originatingNode->GetChunkList());
    } else {
        // Set proper originating mode.
        originatingNode->SetUpdateMode(
            originatingMode == NChunkClient::EUpdateMode::Overwrite ||
            branchedMode == NChunkClient::EUpdateMode::Overwrite
            ? NChunkClient::EUpdateMode::Overwrite
            : NChunkClient::EUpdateMode::Append);
    }
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
