#pragma once
#ifndef CHUNK_OWNER_TYPE_HANDLER_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_owner_type_handler.h"
#endif

#include "helpers.h"
#include "chunk_manager.h"

#include <yt/server/cypress_server/node_detail.h>

#include <yt/server/cypress_server/node.h>
#include <yt/server/cypress_server/node_detail.h>
#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/object_server/object_manager.h>

#include <yt/server/cell_master/hydra_facade.h>

#include <yt/ytlib/chunk_client/data_statistics.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkOwner>
TChunkOwnerTypeHandler<TChunkOwner>::TChunkOwnerTypeHandler(NCellMaster::TBootstrap* bootstrap)
    : TBase(bootstrap)
    , Logger(ChunkServerLogger)
{ }

template <class TChunkOwner>
NYTree::ENodeType TChunkOwnerTypeHandler<TChunkOwner>::GetNodeType() const
{
    return NYTree::ENodeType::Entity;
}

template <class TChunkOwner>
NSecurityServer::TClusterResources TChunkOwnerTypeHandler<TChunkOwner>::GetTotalResourceUsage(
    const NCypressServer::TCypressNodeBase* node)
{
    const auto* chunkOwnerNode = node->As<TChunkOwner>();
    auto result = TBase::GetTotalResourceUsage(node);
    auto statistics = chunkOwnerNode->ComputeTotalStatistics();

    result += GetChunkOwnerDiskUsage(statistics, *chunkOwnerNode);
    return result;
}

template <class TChunkOwner>
NSecurityServer::TClusterResources TChunkOwnerTypeHandler<TChunkOwner>::GetAccountingResourceUsage(
    const NCypressServer::TCypressNodeBase* node)
{
    const auto* chunkOwnerNode = node->As<TChunkOwner>();
    NChunkClient::NProto::TDataStatistics statistics;
    if (chunkOwnerNode->GetUpdateMode() == NChunkClient::EUpdateMode::Append) {
        statistics = chunkOwnerNode->DeltaStatistics();
    } else if (
        chunkOwnerNode->GetUpdateMode() == NChunkClient::EUpdateMode::Overwrite ||
        chunkOwnerNode->IsTrunk())
    {
        statistics = chunkOwnerNode->SnapshotStatistics();
    }
    return
        TBase::GetAccountingResourceUsage(node) +
        GetChunkOwnerDiskUsage(statistics, *chunkOwnerNode);
}

template <class TChunkOwner>
NSecurityServer::TClusterResources TChunkOwnerTypeHandler<TChunkOwner>::GetChunkOwnerDiskUsage(
    const NChunkClient::NProto::TDataStatistics& statistics,
    const TChunkOwner& chunkOwner)
{
    NSecurityServer::TClusterResources result;
    for (int mediumIndex = 0; mediumIndex < MaxMediumCount; ++mediumIndex) {
        result.DiskSpace[mediumIndex] = CalculateDiskSpaceUsage(
            chunkOwner.Properties()[mediumIndex].GetReplicationFactor(),
            statistics.regular_disk_space(),
            statistics.erasure_disk_space());
    }
    result.ChunkCount = statistics.chunk_count();
    return result;
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::InitializeAttributes(NYTree::IAttributeDictionary* attributes)
{
    if (!attributes->Contains("replication_factor")) {
        attributes->Set("replication_factor", GetDefaultReplicationFactor());
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
    NSecurityServer::TAccount* account,
    bool enableAccounting)
{
    const auto& chunkManager = this->Bootstrap_->GetChunkManager();
    const auto& objectManager = this->Bootstrap_->GetObjectManager();

    auto nodeHolder = TBase::DoCreate(
        id,
        externalCellTag,
        transaction,
        attributes,
        account,
        enableAccounting);
    auto* node = nodeHolder.get();

    if (!node->IsExternal()) {
        // Create an empty chunk list and reference it from the node.
        auto* chunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
        node->SetChunkList(chunkList);
        chunkList->AddOwningNode(node);
        objectManager->RefObject(chunkList);
    }

    return nodeHolder;
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoDestroy(TChunkOwner* node)
{
    TBase::DoDestroy(node);

    auto* chunkList = node->GetChunkList();
    if (chunkList) {
        if (node->IsTrunk()) {
            const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
            chunkManager->ScheduleChunkPropertiesUpdate(chunkList);
        }

        chunkList->RemoveOwningNode(node);

        const auto& objectManager = TBase::Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(chunkList);
    }
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoBranch(
    const TChunkOwner* originatingNode,
    TChunkOwner* branchedNode,
    NCypressServer::ELockMode mode)
{
    TBase::DoBranch(originatingNode, branchedNode, mode);

    if (!originatingNode->IsExternal()) {
        auto* chunkList = originatingNode->GetChunkList();
        branchedNode->SetChunkList(chunkList);

        branchedNode->GetChunkList()->AddOwningNode(branchedNode);

        const auto& objectManager = TBase::Bootstrap_->GetObjectManager();
        objectManager->RefObject(chunkList);
    }

    branchedNode->SetPrimaryMediumIndex(originatingNode->GetPrimaryMediumIndex());
    branchedNode->Properties() = originatingNode->Properties();
    branchedNode->SnapshotStatistics() = originatingNode->ComputeTotalStatistics();
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoLogBranch(
    const TChunkOwner* originatingNode,
    TChunkOwner* branchedNode,
    NCypressServer::ELockMode mode)
{
    const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
    const auto* primaryMedium = chunkManager->GetMediumByIndex(originatingNode->GetPrimaryMediumIndex());
    LOG_DEBUG_UNLESS(
        TBase::IsRecovery(),
        "Node branched (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v, "
        "Mode: %v, PrimaryMedium: %v, Properties: %v)",
        originatingNode->GetVersionedId(),
        branchedNode->GetVersionedId(),
        NObjectServer::GetObjectId(originatingNode->GetChunkList()),
        primaryMedium->GetName(),
        originatingNode->Properties(),
        mode);
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoMerge(
    TChunkOwner* originatingNode,
    TChunkOwner* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    // Merge builtin attributes.
    originatingNode->MergeCompressionCodec(originatingNode, branchedNode);
    originatingNode->MergeErasureCodec(originatingNode, branchedNode);

    bool isExternal = originatingNode->IsExternal();

    const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
    const auto& objectManager = TBase::Bootstrap_->GetObjectManager();

    auto* originatingChunkList = originatingNode->GetChunkList();
    auto* branchedChunkList = branchedNode->GetChunkList();

    auto originatingMode = originatingNode->GetUpdateMode();
    auto branchedMode = branchedNode->GetUpdateMode();

    if (!isExternal) {
        branchedChunkList->RemoveOwningNode(branchedNode);
    }

    // Check if we have anything to do at all.
    if (branchedMode == NChunkClient::EUpdateMode::None) {
        if (!isExternal) {
            objectManager->UnrefObject(branchedChunkList);
        }
        return;
    }

    bool topmostCommit = !originatingNode->GetTransaction();
    bool propertiesMismatch = originatingNode->Properties() != branchedNode->Properties();
    bool propertiesUpdateNeeded =
        topmostCommit &&
        (propertiesMismatch || branchedNode->GetChunkPropertiesUpdateNeeded());
    auto newOriginatingMode = topmostCommit || originatingNode->GetType() == NObjectClient::EObjectType::Journal
        ? NChunkClient::EUpdateMode::None
        : originatingMode == NChunkClient::EUpdateMode::Overwrite || branchedMode == NChunkClient::EUpdateMode::Overwrite
            ? NChunkClient::EUpdateMode::Overwrite
            : NChunkClient::EUpdateMode::Append;

    if (branchedMode == NChunkClient::EUpdateMode::Overwrite) {
        if (!isExternal) {
            originatingChunkList->RemoveOwningNode(originatingNode);
            branchedChunkList->AddOwningNode(originatingNode);
            originatingNode->SetChunkList(branchedChunkList);

            if (propertiesUpdateNeeded) {
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
            newOriginatingChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);

            originatingChunkList->RemoveOwningNode(originatingNode);
            newOriginatingChunkList->AddOwningNode(originatingNode);
            originatingNode->SetChunkList(newOriginatingChunkList);
            objectManager->RefObject(newOriginatingChunkList);
        }

        if (originatingMode == NChunkClient::EUpdateMode::Append) {
            YCHECK(!topmostCommit); // No need to update properties.
            if (!isExternal) {
                chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList->Children()[0]);
                auto* newDeltaChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                chunkManager->AttachToChunkList(newOriginatingChunkList, newDeltaChunkList);
                chunkManager->AttachToChunkList(newDeltaChunkList, originatingChunkList->Children()[1]);
                chunkManager->AttachToChunkList(newDeltaChunkList, deltaTree);
            }

            originatingNode->DeltaStatistics() += branchedNode->DeltaStatistics();
        } else {
            if (!isExternal) {
                chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList);
                chunkManager->AttachToChunkList(newOriginatingChunkList, deltaTree);

                if (propertiesUpdateNeeded) {
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

    if (!topmostCommit && branchedNode->GetChunkPropertiesUpdateNeeded()) {
        originatingNode->SetChunkPropertiesUpdateNeeded(true);
    }

    auto* newOriginatingChunkList = originatingNode->GetChunkList();

    if (topmostCommit && !isExternal) {
        // Rebalance when the topmost transaction commits.
        chunkManager->RebalanceChunkTree(newOriginatingChunkList);
    }

    originatingNode->SetUpdateMode(newOriginatingMode);
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoLogMerge(
    TChunkOwner* originatingNode,
    TChunkOwner* branchedNode)
{
    const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
    const auto* originatingPrimaryMedium = chunkManager->GetMediumByIndex(originatingNode->GetPrimaryMediumIndex());
    const auto* branchedPrimaryMedium = chunkManager->GetMediumByIndex(branchedNode->GetPrimaryMediumIndex());
    LOG_DEBUG_UNLESS(
        TBase::IsRecovery(),
        "Node merged (OriginatingNodeId: %v, OriginatingPrimaryMedium: %v, "
        "OriginatingProperties: %v, BranchedNodeId: %v, BranchedChunkListId: %v, "
        "BranchedUpdateMode: %v, BranchedPrimaryMedium: %v, BranchedProperties: %v, "
        "NewOriginatingChunkListId: %v, NewOriginatingUpdateMode: %v)",
        originatingNode->GetVersionedId(),
        originatingPrimaryMedium->GetName(),
        originatingNode->Properties(),
        branchedNode->GetVersionedId(),
        NObjectServer::GetObjectId(branchedNode->GetChunkList()),
        branchedNode->GetUpdateMode(),
        branchedPrimaryMedium->GetName(),
        branchedNode->Properties(),
        NObjectServer::GetObjectId(originatingNode->GetChunkList()),
        originatingNode->GetUpdateMode());
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoClone(
    TChunkOwner* sourceNode,
    TChunkOwner* clonedNode,
    NCypressServer::ICypressNodeFactory* factory,
    NCypressServer::ENodeCloneMode mode,
    NSecurityServer::TAccount* account)
{
    TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

    if (!sourceNode->IsExternal()) {
        const auto& objectManager = TBase::Bootstrap_->GetObjectManager();
        auto* chunkList = sourceNode->GetChunkList();
        YCHECK(!clonedNode->GetChunkList());
        clonedNode->SetChunkList(chunkList);
        objectManager->RefObject(chunkList);
        chunkList->AddOwningNode(clonedNode);
    }

    clonedNode->SetPrimaryMediumIndex(sourceNode->GetPrimaryMediumIndex());
    clonedNode->Properties() = sourceNode->Properties();
    clonedNode->SnapshotStatistics() = sourceNode->SnapshotStatistics();
    clonedNode->DeltaStatistics() = sourceNode->DeltaStatistics();
    clonedNode->SetCompressionCodec(sourceNode->GetCompressionCodec());
    clonedNode->SetErasureCodec(sourceNode->GetErasureCodec());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
