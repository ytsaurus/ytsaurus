#pragma once
#ifndef CHUNK_OWNER_TYPE_HANDLER_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_owner_type_handler.h"
// For the sake of sane code completion.
#include "chunk_owner_type_handler.h"
#endif

#include "chunk_manager.h"
#include "helpers.h"

#include <yt/server/master/cypress_server/node_detail.h>

#include <yt/server/master/cypress_server/node.h>
#include <yt/server/master/cypress_server/node_detail.h>
#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/master/chunk_server/chunk_manager.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/security_tags.h>

#include <yt/server/master/tablet_server/tablet_manager.h>

#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/core/ytree/interned_attributes.h>

namespace NYT::NChunkServer {

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
bool TChunkOwnerTypeHandler<TChunkOwner>::IsSupportedInheritableAttribute(const TString& key) const
{
    static const THashSet<TString> supportedInheritableAttributes = {
        "compression_codec",
        "erasure_codec",
        "media"
        "primary_medium",
        "replication_factor",
        "vital"
    };

    return supportedInheritableAttributes.contains(key);
}

template <class TChunkOwner>
bool TChunkOwnerTypeHandler<TChunkOwner>::HasBranchedChangesImpl(TChunkOwner* originatingNode, TChunkOwner* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
        return true;
    }

    return
        branchedNode->GetUpdateMode() != NChunkClient::EUpdateMode::None ||
        branchedNode->GetPrimaryMediumIndex() != originatingNode->GetPrimaryMediumIndex() ||
        branchedNode->Replication() != originatingNode->Replication() ||
        branchedNode->GetCompressionCodec() != originatingNode->GetCompressionCodec() ||
        branchedNode->GetErasureCodec() != originatingNode->GetErasureCodec() ||
        !branchedNode->DeltaSecurityTags()->IsEmpty() ||
        !NSecurityServer::TInternedSecurityTags::RefEqual(branchedNode->SnapshotSecurityTags(), originatingNode->SnapshotSecurityTags());
}

template <class TChunkOwner>
std::unique_ptr<TChunkOwner> TChunkOwnerTypeHandler<TChunkOwner>::DoCreateImpl(
    const NCypressServer::TVersionedNodeId& id,
    NObjectClient::TCellTag externalCellTag,
    NTransactionServer::TTransaction* transaction,
    NYTree::IAttributeDictionary* inheritedAttributes,
    NYTree::IAttributeDictionary* explicitAttributes,
    NSecurityServer::TAccount* account,
    int replicationFactor,
    NCompression::ECodec compressionCodec,
    NErasure::ECodec erasureCodec)
{
    const auto& chunkManager = this->Bootstrap_->GetChunkManager();

    auto combinedAttributes = NYTree::OverlayAttributeDictionaries(explicitAttributes, inheritedAttributes);

    auto primaryMediumName = combinedAttributes.GetAndRemove<TString>("primary_medium", NChunkClient::DefaultStoreMediumName);
    auto* primaryMedium = chunkManager->GetMediumByNameOrThrow(primaryMediumName);

    std::optional<NSecurityServer::TSecurityTags> securityTags;
    auto securityTagItems = combinedAttributes.FindAndRemove<NSecurityServer::TSecurityTagsItems>("security_tags");
    if (securityTagItems) {
        securityTags = NSecurityServer::TSecurityTags{std::move(*securityTagItems)};
        securityTags->Validate();
    }

    auto nodeHolder = TBase::DoCreate(
        id,
        externalCellTag,
        transaction,
        inheritedAttributes,
        explicitAttributes,
        account);
    auto* node = nodeHolder.get();

    try {
        node->SetPrimaryMediumIndex(primaryMedium->GetIndex());

        node->Replication().Set(primaryMedium->GetIndex(), TReplicationPolicy(replicationFactor, false));

        node->SetCompressionCodec(compressionCodec);
        node->SetErasureCodec(erasureCodec);

        if (securityTags) {
            const auto& securityManager = this->Bootstrap_->GetSecurityManager();
            const auto& securityTagsRegistry = securityManager->GetSecurityTagsRegistry();
            node->SnapshotSecurityTags() = securityTagsRegistry->Intern(std::move(*securityTags));
        }

        if (!node->IsExternal()) {
            // Create an empty chunk list and reference it from the node.
            auto* chunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
            node->SetChunkList(chunkList);
            chunkList->AddOwningNode(node);

            const auto& objectManager = this->Bootstrap_->GetObjectManager();
            objectManager->RefObject(chunkList);
        }
    } catch (const std::exception&) {
        DoDestroy(node);
        throw;
    }

    return nodeHolder;
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoDestroy(TChunkOwner* node)
{
    TBase::DoDestroy(node);

    auto* chunkList = node->GetChunkList();
    if (chunkList) {
        if (!node->IsExternal() && node->IsTrunk()) {
            const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
            chunkManager->ScheduleChunkRequisitionUpdate(chunkList);
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
    const NCypressServer::TLockRequest& lockRequest)
{
    TBase::DoBranch(originatingNode, branchedNode, lockRequest);

    if (!originatingNode->IsExternal()) {
        auto* chunkList = originatingNode->GetChunkList();
        branchedNode->SetChunkList(chunkList);

        branchedNode->GetChunkList()->AddOwningNode(branchedNode);

        const auto& objectManager = TBase::Bootstrap_->GetObjectManager();
        objectManager->RefObject(chunkList);
    }

    branchedNode->SetPrimaryMediumIndex(originatingNode->GetPrimaryMediumIndex());
    branchedNode->Replication() = originatingNode->Replication();
    branchedNode->SnapshotStatistics() = originatingNode->ComputeTotalStatistics();

    if (originatingNode->DeltaSecurityTags()->IsEmpty()) {
        // Fast path.
        branchedNode->SnapshotSecurityTags() = originatingNode->SnapshotSecurityTags();
    } else {
        // Slow path.
        const auto& securityManager = TBase::Bootstrap_->GetSecurityManager();
        const auto& securityTagsRegistry = securityManager->GetSecurityTagsRegistry();
        branchedNode->SnapshotSecurityTags() = securityTagsRegistry->Intern(originatingNode->GetSecurityTags());
    }
}

template <class TChunkOwner>
void TChunkOwnerTypeHandler<TChunkOwner>::DoLogBranch(
    const TChunkOwner* originatingNode,
    TChunkOwner* branchedNode,
    const NCypressServer::TLockRequest& lockRequest)
{
    const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
    const auto* primaryMedium = chunkManager->GetMediumByIndex(originatingNode->GetPrimaryMediumIndex());
    YT_LOG_DEBUG_UNLESS(
        TBase::IsRecovery(),
        "Node branched (OriginatingNodeId: %v, BranchedNodeId: %v, ChunkListId: %v, "
        "PrimaryMedium: %v, Replication: %v, Mode: %v, LockTimestamp: %llx)",
        originatingNode->GetVersionedId(),
        branchedNode->GetVersionedId(),
        NObjectServer::GetObjectId(originatingNode->GetChunkList()),
        primaryMedium->GetName(),
        originatingNode->Replication(),
        lockRequest.Mode,
        lockRequest.Timestamp);
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
    const auto& securityManager = TBase::Bootstrap_->GetSecurityManager();
    const auto& securityTagsRegistry = securityManager->GetSecurityTagsRegistry();

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
    auto newOriginatingMode = topmostCommit || originatingNode->GetType() == NObjectClient::EObjectType::Journal
        ? NChunkClient::EUpdateMode::None
        : originatingMode == NChunkClient::EUpdateMode::Overwrite || branchedMode == NChunkClient::EUpdateMode::Overwrite
            ? NChunkClient::EUpdateMode::Overwrite
            : NChunkClient::EUpdateMode::Append;

    // For new chunks, there're two reasons to update chunk requisition.
    //
    // 1) To ensure proper replicator behavior. This is only needed for topmost
    // commits, and only when nodes' replication settings differ.
    //
    // 2) To ensure proper resource accounting. This is necessary (A) for all
    // topmost commits (since committed and uncommitted resources are tracked
    // separately) and (B) for nested commits when replication changes (NB: node
    // accounts cannot be changed within transactions and are therefore
    // irrelevant).
    //
    // For old chunks, requisition update is only needed iff they're being
    // overwritten. (NB: replication settings changes are never merged back to
    // the originating node and thus have no effect on these chunks.)

    auto requisitionUpdateNeeded = topmostCommit || originatingNode->Replication() != branchedNode->Replication();

    // Below, chunk requisition update is scheduled no matter what (for non-external chunks,
    // of course). If nothing else, this is necessary to update 'committed' flags on chunks.

    if (branchedMode == NChunkClient::EUpdateMode::Overwrite) {
        if (!isExternal) {
            YT_VERIFY(branchedChunkList->GetKind() == EChunkListKind::Static);

            originatingChunkList->RemoveOwningNode(originatingNode);
            branchedChunkList->AddOwningNode(originatingNode);
            originatingNode->SetChunkList(branchedChunkList);

            chunkManager->ScheduleChunkRequisitionUpdate(originatingChunkList);
            if (requisitionUpdateNeeded) {
                chunkManager->ScheduleChunkRequisitionUpdate(branchedChunkList);
            }

            objectManager->UnrefObject(originatingChunkList);
        }

        originatingNode->SnapshotStatistics() = branchedNode->SnapshotStatistics();
        originatingNode->DeltaStatistics() = branchedNode->DeltaStatistics();
        originatingNode->SnapshotSecurityTags() = branchedNode->SnapshotSecurityTags();
        originatingNode->DeltaSecurityTags() = branchedNode->DeltaSecurityTags();
    } else {
        YT_VERIFY(branchedMode == NChunkClient::EUpdateMode::Append);

        TChunkTree* deltaTree = nullptr;
        TChunkList* newOriginatingChunkList = nullptr;
        if (!isExternal) {
            if (branchedChunkList->GetKind() == EChunkListKind::SortedDynamicRoot) {
                if (originatingNode->IsTrunk()) {
                    if (branchedChunkList != originatingChunkList) {
                        const auto& tabletManager = TBase::Bootstrap_->GetTabletManager();
                        tabletManager->MergeTableNodes(originatingNode, branchedNode);
                    }

                    objectManager->UnrefObject(branchedChunkList);
                } else {
                    // For non-trunk node just overwrite originating node with branched node contents.
                    // Could be made more consistent with static tables by using hierarchical chunk lists.

                    originatingNode->SetChunkList(branchedChunkList);
                    originatingChunkList->RemoveOwningNode(originatingNode);
                    branchedChunkList->AddOwningNode(originatingNode);
                    objectManager->UnrefObject(originatingChunkList);
                }
            } else {
                YT_VERIFY(branchedChunkList->Children().size() == 2);
                deltaTree = branchedChunkList->Children()[1];
                newOriginatingChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);

                originatingChunkList->RemoveOwningNode(originatingNode);
                newOriginatingChunkList->AddOwningNode(originatingNode);
                originatingNode->SetChunkList(newOriginatingChunkList);
                objectManager->RefObject(newOriginatingChunkList);
            }
        }

        if (originatingMode == NChunkClient::EUpdateMode::Append) {
            YT_VERIFY(!topmostCommit);
            if (!isExternal && branchedChunkList->GetKind() == EChunkListKind::Static) {
                chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList->Children()[0]);
                auto* newDeltaChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                chunkManager->AttachToChunkList(newOriginatingChunkList, newDeltaChunkList);
                chunkManager->AttachToChunkList(newDeltaChunkList, originatingChunkList->Children()[1]);
                chunkManager->AttachToChunkList(newDeltaChunkList, deltaTree);
            }

            originatingNode->DeltaStatistics() += branchedNode->DeltaStatistics();
            originatingNode->DeltaSecurityTags() = securityTagsRegistry->Intern(
                *originatingNode->DeltaSecurityTags() + *branchedNode->DeltaSecurityTags());
        } else {
            if (!isExternal && branchedChunkList->GetKind() == EChunkListKind::Static) {
                YT_VERIFY(originatingChunkList->GetKind() == EChunkListKind::Static);

                chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList);
                chunkManager->AttachToChunkList(newOriginatingChunkList, deltaTree);

                if (requisitionUpdateNeeded) {
                    chunkManager->ScheduleChunkRequisitionUpdate(deltaTree);
                }
            }

            if (newOriginatingMode == NChunkClient::EUpdateMode::Append) {
                originatingNode->DeltaStatistics() += branchedNode->DeltaStatistics();
                originatingNode->DeltaSecurityTags() = securityTagsRegistry->Intern(
                    *originatingNode->DeltaSecurityTags() + *branchedNode->DeltaSecurityTags());
            } else {
                originatingNode->SnapshotStatistics() += branchedNode->DeltaStatistics();
                originatingNode->SnapshotSecurityTags() = securityTagsRegistry->Intern(
                    *originatingNode->SnapshotSecurityTags() + *branchedNode->DeltaSecurityTags());
            }
        }

        if (!isExternal && branchedChunkList->GetKind() == EChunkListKind::Static) {
            objectManager->UnrefObject(originatingChunkList);
            objectManager->UnrefObject(branchedChunkList);
        }
    }

    auto* newOriginatingChunkList = originatingNode->GetChunkList();

    if (topmostCommit && !isExternal && branchedChunkList->GetKind() == EChunkListKind::Static) {
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
    YT_LOG_DEBUG_UNLESS(
        TBase::IsRecovery(),
        "Node merged (OriginatingNodeId: %v, OriginatingPrimaryMedium: %v, "
        "OriginatingReplication: %v, BranchedNodeId: %v, BranchedChunkListId: %v, "
        "BranchedUpdateMode: %v, BranchedPrimaryMedium: %v, BranchedReplication: %v, "
        "NewOriginatingChunkListId: %v, NewOriginatingUpdateMode: %v)",
        originatingNode->GetVersionedId(),
        originatingPrimaryMedium->GetName(),
        originatingNode->Replication(),
        branchedNode->GetVersionedId(),
        NObjectServer::GetObjectId(branchedNode->GetChunkList()),
        branchedNode->GetUpdateMode(),
        branchedPrimaryMedium->GetName(),
        branchedNode->Replication(),
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

    clonedNode->SetPrimaryMediumIndex(sourceNode->GetPrimaryMediumIndex());
    clonedNode->Replication() = sourceNode->Replication();
    clonedNode->SnapshotStatistics() = sourceNode->SnapshotStatistics();
    clonedNode->DeltaStatistics() = sourceNode->DeltaStatistics();
    clonedNode->SnapshotSecurityTags() = sourceNode->SnapshotSecurityTags();
    clonedNode->DeltaSecurityTags() = sourceNode->DeltaSecurityTags();
    clonedNode->SetCompressionCodec(sourceNode->GetCompressionCodec());
    clonedNode->SetErasureCodec(sourceNode->GetErasureCodec());

    if (!sourceNode->IsExternal()) {
        const auto& objectManager = TBase::Bootstrap_->GetObjectManager();
        auto* chunkList = sourceNode->GetChunkList();
        YT_VERIFY(!clonedNode->GetChunkList());
        clonedNode->SetChunkList(chunkList);
        objectManager->RefObject(chunkList);
        chunkList->AddOwningNode(clonedNode);
        if (clonedNode->IsTrunk() && sourceNode->GetAccount() != clonedNode->GetAccount()) {
            const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
            chunkManager->ScheduleChunkRequisitionUpdate(chunkList);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
