#include "store_manager_detail.h"

#include "config.h"
#include "hunk_chunk.h"
#include "in_memory_manager.h"
#include "ordered_dynamic_store.h"
#include "private.h"
#include "serialize.h"
#include "store.h"
#include "structured_logger.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "transaction.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/utilex/random.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NHydra;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NCypressClient;

using NLsm::EStoreRotationReason;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TStoreManagerBase::TStoreManagerBase(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    ITabletContext* tabletContext,
    NHydra::IHydraManagerPtr hydraManager,
    IInMemoryManagerPtr inMemoryManager,
    NNative::IClientPtr client)
    : Tablet_(tablet)
    , Config_(config)
    , TabletContext_(tabletContext)
    , HydraManager_(std::move(hydraManager))
    , InMemoryManager_(std::move(inMemoryManager))
    , Client_(std::move(client))
    , StructuredLogger_(tablet->GetStructuredLogger())
    , Logger(TabletNodeLogger())
{
    YT_VERIFY(StructuredLogger_);

    Logger.AddTag("%v, CellId: %v",
        Tablet_->GetLoggingTag(),
        TabletContext_->GetCellId());
}

bool TStoreManagerBase::HasActiveLocks() const
{
    const auto* activeStore = GetActiveStore();
    if (activeStore && activeStore->GetLockCount() > 0) {
        return true;
    }

    if (!LockedStores_.empty()) {
        return true;
    }

    return false;
}

bool TStoreManagerBase::HasUnflushedStores() const
{
    for (const auto& [storeId, store] : Tablet_->StoreIdMap()) {
        if (store->GetStoreState() != EStoreState::Persistent) {
            return true;
        }
    }
    return false;
}

void TStoreManagerBase::StartEpoch(ITabletSlotPtr slot)
{
    if (auto delay = Tablet_->GetSettings().MountConfig->Testing.SimulatedTabletSnapshotDelay) {
        // For integration testing.
        Sleep(delay);
    }

    Tablet_->StartEpoch(slot);

    InitializeRotation();

    UpdateInMemoryMode();
}

void TStoreManagerBase::StopEpoch()
{
    Tablet_->StopEpoch();

    for (const auto& [storeId, store] : Tablet_->StoreIdMap()) {
        if (store->IsDynamic()) {
            store->AsDynamic()->SetFlushState(EStoreFlushState::None);
            StructuredLogger_->OnStoreFlushStateChanged(store->AsDynamic());
        }
        if (store->IsChunk()) {
            auto chunkStore = store->AsChunk();
            chunkStore->SetCompactionState(EStoreCompactionState::None);
            StructuredLogger_->OnStoreCompactionStateChanged(chunkStore);

            if (chunkStore->GetPreloadState() == EStorePreloadState::Scheduled ||
                chunkStore->GetPreloadState() == EStorePreloadState::Running)
            {
                // Running preloads are cancelled in cancellable invoker. There are no
                // concurrent preloads when this code is running, because execution is
                // serialized in one thread.
                chunkStore->SetPreloadState(EStorePreloadState::None);
                StructuredLogger_->OnStorePreloadStateChanged(chunkStore);
            }
        }
    }

    for (const auto& [chunkId, hunkChunk] : Tablet_->HunkChunkMap()) {
        hunkChunk->SetSweepState(EHunkChunkSweepState::None);
    }

    Tablet_->PreloadStoreIds().clear();
}

void TStoreManagerBase::InitializeRotation()
{
    ResetLastPeriodicRotationTime();
    RotationScheduled_ = false;
}

bool TStoreManagerBase::IsRotationScheduled() const
{
    return RotationScheduled_;
}

void TStoreManagerBase::ScheduleRotation(EStoreRotationReason reason)
{
    if (RotationScheduled_)
        return;

    RotationScheduled_ = true;

    YT_LOG_INFO("Tablet store rotation scheduled (Reason: %v)", reason);

    auto* activeStore = GetActiveStore();
    if (reason == EStoreRotationReason::None || !activeStore) {
        return;
    }

    Tablet_->GetTableProfiler()->GetLsmCounters()->ProfileRotation(
        reason,
        activeStore->GetRowCount(),
        activeStore->GetDynamicMemoryUsage());
}

void TStoreManagerBase::UnscheduleRotation()
{
    YT_LOG_DEBUG("Tablet store rotation allowed after unsuccessful rotation attempt");
    RotationScheduled_ = false;
}

void TStoreManagerBase::AddStore(
    IStorePtr store,
    TAddStoreOptions options)
{
    Tablet_->AddStore(store, options.OnFlush, options.PartitionIdHint);

    if (store->IsChunk()) {
        auto chunkStore = store->AsChunk();
        if (chunkStore->GetPreloadState() == EStorePreloadState::Scheduled) {
            bool shouldSchedulePreload = true;

            if (options.UseInterceptedChunkData || options.UseRetainedPreloadedChunks) {
                auto chunkData = options.UseRetainedPreloadedChunks
                    ? InMemoryManager_->GetInterceptedChunkData(chunkStore->GetChunkId())
                    : InMemoryManager_->EvictInterceptedChunkData(chunkStore->GetChunkId());
                if (TryPreloadStoreFromInterceptedData(chunkStore, chunkData)) {
                    shouldSchedulePreload = false;
                }
            }

            if (shouldSchedulePreload) {
                YT_LOG_INFO("Scheduled preload of in-memory store (StoreId: %v)", store->GetId());
                Tablet_->PreloadStoreIds().push_back(store->GetId());
            }
        }
    }
}

void TStoreManagerBase::BulkAddStores(TRange<IStorePtr> stores)
{
    TBulkInsertProfiler bulkInsertProfiler(Tablet_);
    for (auto store : stores) {
        bulkInsertProfiler.Update(store);
        AddStore(std::move(store), /*options*/ {});
    }
}

void TStoreManagerBase::DiscardAllStores()
{
    std::vector<IStorePtr> storesToRemove;

    for (auto [id, store] : Tablet_->StoreIdMap()) {
        if (store->GetStoreState() != EStoreState::ActiveDynamic) {
            storesToRemove.push_back(store);
        }
    }

    for (const auto& store : storesToRemove) {
        RemoveStore(store);
    }

    const auto* context = GetCurrentMutationContext();

    TVersion version = context->GetVersion();
    // COMPAT(h0pless): HydraLogicalRecordId.
    auto mutationReign = static_cast<ETabletReign>(context->Request().Reign);
    if (mutationReign < ETabletReign::HydraLogicalRecordId) {
        version = context->GetPhysicalVersion();
    }

    Tablet_->SetLastDiscardStoresRevision(version.ToRevision());
}

void TStoreManagerBase::RemoveStore(IStorePtr store)
{
    YT_ASSERT(store->GetStoreState() != EStoreState::ActiveDynamic);

    store->SetStoreState(EStoreState::Removed);
    StructuredLogger_->OnStoreStateChanged(store);
    Tablet_->RemoveStore(store);
}

void TStoreManagerBase::BackoffStoreRemoval(IStorePtr store)
{
    switch (store->GetType()) {
        case EStoreType::SortedDynamic:
        case EStoreType::OrderedDynamic: {
            auto dynamicStore = store->AsDynamic();
            auto flushState = dynamicStore->GetFlushState();
            if (flushState == EStoreFlushState::Complete) {
                dynamicStore->SetFlushState(EStoreFlushState::None);
                dynamicStore->UpdateFlushAttemptTimestamp();
                StructuredLogger_->OnStoreFlushStateChanged(dynamicStore);
            }
            break;
        }
        case EStoreType::SortedChunk:
        case EStoreType::OrderedChunk: {
            auto chunkStore = store->AsChunk();
            auto compactionState = chunkStore->GetCompactionState();
            if (compactionState == EStoreCompactionState::Complete) {
                chunkStore->SetCompactionState(EStoreCompactionState::None);
                StructuredLogger_->OnStoreCompactionStateChanged(chunkStore);
                chunkStore->UpdateCompactionAttempt();
            }
            break;
        }

        default:
            break;
    }
}

bool TStoreManagerBase::IsStoreFlushable(IStorePtr store) const
{
    if (store->GetStoreState() != EStoreState::PassiveDynamic) {
        return false;
    }

    auto dynamicStore = store->AsDynamic();
    if (dynamicStore->GetFlushState() != EStoreFlushState::None) {
        return false;
    }

    if (dynamicStore->GetLastFlushAttemptTimestamp() + Config_->FlushBackoffTime > Now()) {
        return false;
    }

    if (!Tablet_->GetSettings().MountConfig->EnableStoreFlush) {
        return false;
    }

    return true;
}

TStoreFlushCallback TStoreManagerBase::BeginStoreFlush(
    IDynamicStorePtr store,
    TTabletSnapshotPtr tabletSnapshot,
    bool isUnmountWorkflow,
    bool onlyUpdateRowCache)
{
    YT_VERIFY(store->GetFlushState() == EStoreFlushState::None);
    store->SetFlushState(EStoreFlushState::Running);
    StructuredLogger_->OnStoreFlushStateChanged(store);
    return MakeStoreFlushCallback(store, tabletSnapshot, isUnmountWorkflow, onlyUpdateRowCache);
}

void TStoreManagerBase::EndStoreFlush(IDynamicStorePtr store)
{
    YT_VERIFY(store->GetFlushState() == EStoreFlushState::Running);
    store->SetFlushState(EStoreFlushState::Complete);
    StructuredLogger_->OnStoreFlushStateChanged(store);
}

void TStoreManagerBase::BackoffStoreFlush(IDynamicStorePtr store)
{
    YT_VERIFY(store->GetFlushState() == EStoreFlushState::Running);
    store->SetFlushState(EStoreFlushState::None);
    StructuredLogger_->OnStoreFlushStateChanged(store);
    store->UpdateFlushAttemptTimestamp();
}

void TStoreManagerBase::BeginStoreCompaction(IChunkStorePtr store)
{
    YT_VERIFY(store->GetCompactionState() == EStoreCompactionState::None);
    store->SetCompactionState(EStoreCompactionState::Running);
    StructuredLogger_->OnStoreCompactionStateChanged(store);
}

void TStoreManagerBase::EndStoreCompaction(IChunkStorePtr store)
{
    YT_VERIFY(store->GetCompactionState() == EStoreCompactionState::Running);
    store->SetCompactionState(EStoreCompactionState::Complete);
    StructuredLogger_->OnStoreCompactionStateChanged(store);
}

void TStoreManagerBase::BackoffStoreCompaction(IChunkStorePtr store)
{
    YT_VERIFY(store->GetCompactionState() == EStoreCompactionState::Running);
    store->SetCompactionState(EStoreCompactionState::None);
    StructuredLogger_->OnStoreCompactionStateChanged(store);
    store->UpdateCompactionAttempt();
}

bool TStoreManagerBase::TryPreloadStoreFromInterceptedData(
    IChunkStorePtr store,
    TInMemoryChunkDataPtr chunkData)
{
    if (!chunkData) {
        YT_LOG_WARNING(
            "Intercepted chunk data for in-memory store is missing (StoreId: %v)",
            store->GetId());
        return false;
    }

    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    auto mode = mountConfig->InMemoryMode;
    if (mode != chunkData->InMemoryMode) {
        YT_LOG_WARNING(
            "Intercepted chunk data for in-memory store has invalid mode (StoreId: %v, ExpectedMode: %v, ActualMode: %v)",
            store->GetId(),
            mode,
            chunkData->InMemoryMode);
        return false;
    }

    store->Preload(chunkData);
    store->SetPreloadState(EStorePreloadState::Complete);
    StructuredLogger_->OnStorePreloadStateChanged(store);

    YT_LOG_INFO("In-memory store preloaded from intercepted chunk data (StoreId: %v, Mode: %v)",
        store->GetId(),
        mode);

    return true;
}

IChunkStorePtr TStoreManagerBase::PeekStoreForPreload()
{
    YT_LOG_TRACE("Peeking store for preload");

    for (size_t size = Tablet_->PreloadStoreIds().size(); size != 0; --size) {
        auto id = Tablet_->PreloadStoreIds().front();
        auto store = Tablet_->FindStore(id);
        if (store) {
            auto chunkStore = store->AsChunk();
            if (chunkStore->GetPreloadState() == EStorePreloadState::Scheduled) {
                if (chunkStore->IsPreloadAllowed()) {
                    YT_LOG_DEBUG("Peeked store for preload (StoreId: %v)", chunkStore->GetId());
                    return chunkStore;
                } else {
                    YT_LOG_DEBUG("Store preload is not allowed (StoreId: %v)", chunkStore->GetId());
                }
                Tablet_->PreloadStoreIds().pop_front();
                Tablet_->PreloadStoreIds().push_back(id);
                continue;
            } else {
                YT_LOG_DEBUG("Store preload is not scheduled (StoreId: %v)", chunkStore->GetId());
            }
        }
        Tablet_->PreloadStoreIds().pop_front();
    }
    return nullptr;
}

void TStoreManagerBase::BeginStorePreload(IChunkStorePtr store, TCallback<TFuture<void>()> callbackFuture)
{
    YT_VERIFY(store->GetId() == Tablet_->PreloadStoreIds().front());
    Tablet_->PreloadStoreIds().pop_front();

    YT_VERIFY(store->GetPreloadState() == EStorePreloadState::Scheduled);
    store->SetPreloadState(EStorePreloadState::Running);
    StructuredLogger_->OnStorePreloadStateChanged(store);
    store->SetPreloadFuture(callbackFuture.Run());
}

void TStoreManagerBase::EndStorePreload(IChunkStorePtr store)
{
    if (auto delay = Tablet_->GetSettings().MountConfig->Testing.SimulatedStorePreloadDelay) {
        // For integration testing.
        NConcurrency::TDelayedExecutor::WaitForDuration(delay);
    }

    YT_VERIFY(store->GetPreloadState() == EStorePreloadState::Running);
    store->SetPreloadState(EStorePreloadState::Complete);
    StructuredLogger_->OnStorePreloadStateChanged(store);
    store->SetPreloadFuture(TFuture<void>());
}

void TStoreManagerBase::BackoffStorePreload(IChunkStorePtr store)
{
    YT_ASSERT_INVOKERS_AFFINITY(std::vector{
        Tablet_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Default),
        Tablet_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Mutation)
    });

    YT_VERIFY(store->GetPreloadState() == EStorePreloadState::Running);

    store->SetPreloadFuture(TFuture<void>());
    store->SetPreloadState(EStorePreloadState::Scheduled);
    StructuredLogger_->OnStorePreloadStateChanged(store);
    Tablet_->PreloadStoreIds().push_back(store->GetId());
}

EInMemoryMode TStoreManagerBase::GetInMemoryMode() const
{
    return Tablet_->GetSettings().MountConfig->InMemoryMode;
}

void TStoreManagerBase::Mount(
    TRange<const NTabletNode::NProto::TAddStoreDescriptor*> storeDescriptors,
    TRange<const NTabletNode::NProto::TAddHunkChunkDescriptor*> hunkChunkDescriptors,
    TMountOptions options)
{
    for (const auto* descriptor : hunkChunkDescriptors) {
        auto chunkId = FromProto<TChunkId>(descriptor->chunk_id());
        auto hunkChunk = TabletContext_->CreateHunkChunk(
            Tablet_,
            chunkId,
            descriptor);
        hunkChunk->Initialize();
        Tablet_->AddHunkChunk(std::move(hunkChunk));
    }

    for (const auto* descriptor : storeDescriptors) {
        auto type = FromProto<EStoreType>(descriptor->store_type());
        auto storeId = FromProto<TChunkId>(descriptor->store_id());
        YT_VERIFY(descriptor->has_chunk_meta());
        YT_VERIFY(!descriptor->has_backing_store_id());
        auto store = TabletContext_->CreateStore(
            Tablet_,
            type,
            storeId,
            descriptor);
        store->Initialize();
        AddStore(
            store->AsChunk(),
            TAddStoreOptions{
                .UseRetainedPreloadedChunks = options.UseRetainedPreloadedChunks,
            });

        if (auto chunkStore = store->AsChunk()) {
            for (const auto& ref : chunkStore->HunkChunkRefs()) {
                Tablet_->UpdateHunkChunkRef(ref, +1);
            }
        }

        const auto& extensions = descriptor->chunk_meta().extensions();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(extensions);
        if (miscExt.has_max_timestamp()) {
            Tablet_->UpdateLastCommitTimestamp(miscExt.max_timestamp());
        }
    }

    for (const auto& [_, hunkChunk] : Tablet_->HunkChunkMap()) {
        Tablet_->UpdateDanglingHunkChunks(hunkChunk);
    }

    // NB: Active store must be created _after_ chunk stores to make sure it receives
    // the right starting row index (for ordered tablets only).
    if (options.CreateDynamicStore) {
        CreateActiveStore();
    }

    Tablet_->SetState(ETabletState::Mounted);
}

void TStoreManagerBase::Remount(const TTableSettings& settings)
{
    auto oldMountConfig = Tablet_->GetSettings().MountConfig;

    Tablet_->SetSettings(settings);

    if (oldMountConfig->DynamicStoreAutoFlushPeriod != settings.MountConfig->DynamicStoreAutoFlushPeriod) {
        ResetLastPeriodicRotationTime();
    }

    InvalidateCachedChunkReaders();

    UpdateInMemoryMode();
}

void TStoreManagerBase::PopulateReplicateTabletContentRequest(
    NProto::TReqReplicateTabletContent* request)
{
    auto* replicatableContent = request->mutable_replicatable_content();

    for (auto it : GetSortedIterators(Tablet_->HunkChunkMap())) {
        it->second->PopulateAddHunkChunkDescriptor(replicatableContent->add_hunk_chunks());
    }

    if (const auto& activeStore = Tablet_->GetActiveStore()) {
        ToProto(request->mutable_active_store_id(), activeStore->GetId());
    }

    for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
        ToProto(
            request->add_compression_dictionaries_by_policy(),
            Tablet_->GetCompressionDictionaryId(policy));
    }
}

void TStoreManagerBase::LoadReplicatedContent(
    const NProto::TReqReplicateTabletContent* request)
{
    const auto& replicatableContent = request->replicatable_content();
    auto& movementData = Tablet_->SmoothMovementData();

    auto activeStoreId = FromProto<TStoreId>(request->active_store_id());

    for (const auto& descriptor : replicatableContent.hunk_chunks()) {
        auto chunkId = FromProto<TChunkId>(descriptor.chunk_id());
        auto hunkChunk = TabletContext_->CreateHunkChunk(
            Tablet_,
            chunkId,
            &descriptor);
        hunkChunk->Initialize();
        Tablet_->AddHunkChunk(std::move(hunkChunk));
    }

    auto partitionIds = FromProto<std::vector<TPartitionId>>(request->store_partition_ids());
    if (Tablet_->IsPhysicallySorted()) {
        YT_VERIFY(replicatableContent.stores().size() == ssize(partitionIds));
    } else {
        YT_VERIFY(partitionIds.empty());
    }

    for (const auto& [index, descriptor] : Enumerate(replicatableContent.stores())) {
        auto type = FromProto<EStoreType>(descriptor.store_type());
        auto storeId = FromProto<TChunkId>(descriptor.store_id());

        if (type == EStoreType::OrderedDynamic &&
            storeId != activeStoreId)
        {
            YT_VERIFY(static_cast<int>(index + 1) < replicatableContent.stores().size());
            i64 rowCount = replicatableContent.stores()[index + 1].starting_row_index() - descriptor.starting_row_index();
            YT_VERIFY(rowCount > 0);
            EmplaceOrCrash(movementData.StoreRowCountOverride(), storeId, rowCount);
        }

        if (storeId == activeStoreId) {
            CreateActiveStore(activeStoreId);
        } else {
            auto store = TabletContext_->CreateStore(Tablet_, type, storeId, &descriptor);

            if (store->IsDynamic()) {
                store->SetStoreState(EStoreState::PassiveDynamic);
                InsertOrCrash(movementData.CommonDynamicStoreIds(), store->GetId());

                if (store->IsOrdered()) {
                    store->AsOrdered()->SetStartingRowIndex(descriptor.starting_row_index());
                }
            }

            store->Initialize();
            TAddStoreOptions options;
            if (Tablet_->IsPhysicallySorted()) {
                options.PartitionIdHint = partitionIds[index];
            }

            AddStore(store, options);

            if (store->IsChunk()) {
                for (const auto& ref : store->AsChunk()->HunkChunkRefs()) {
                    Tablet_->UpdateHunkChunkRef(ref, +1);
                }
            }
        }
    }

    // TODO(ifsmirnov): reign promotion is a nuisance for smooth movement, YT-25347.
    // This may be technically incorrect with respect to the persistent state
    // if the size of the enum changes, but still, this is not very important part
    // of the state and snapshot validation alerts should be the worst consequence.
    {
        const auto& policies = TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues();
        int policyCount = std::min<int>(
            request->compression_dictionaries_by_policy().size(),
            ssize(policies));
        for (int index = 0; index < policyCount; ++index) {
            Tablet_->AttachCompressionDictionary(
                policies[index],
                FromProto<TChunkId>(request->compression_dictionaries_by_policy()[index]));
        }
    }

    for (const auto& [_, hunkChunk] : Tablet_->HunkChunkMap()) {
        Tablet_->UpdateDanglingHunkChunks(hunkChunk);
    }

    if (Tablet_->IsPhysicallyOrdered()) {
        Tablet_->UpdateTotalRowCount();
    }
}

void TStoreManagerBase::Rotate(bool createNewStore, EStoreRotationReason reason, bool allowEmptyStore)
{
    RotationScheduled_ = false;
    if (reason != EStoreRotationReason::Periodic) {
        ResetLastPeriodicRotationTime();
    }

    auto* activeStore = GetActiveStore();

    if (activeStore) {
        if (createNewStore && activeStore->GetRowCount() == 0 && reason != EStoreRotationReason::Discard) {
            YT_LOG_ALERT_UNLESS(allowEmptyStore, "Empty dynamic store rotated (StoreId: %v, Reason: %v)",
                activeStore->GetId(),
                reason);
        }

        activeStore->SetStoreState(EStoreState::PassiveDynamic);
        auto nonActiveStoresUnmergedRowCount = Tablet_->GetNonActiveStoresUnmergedRowCount();
        Tablet_->SetNonActiveStoresUnmergedRowCount(nonActiveStoresUnmergedRowCount + activeStore->GetRowCount());

        StructuredLogger_->OnStoreStateChanged(activeStore);

        YT_LOG_INFO("Rotating store (StoreId: %v, Reason: %v, DynamicMemoryUsage: %v, RowCount: %v, TimestampCount: %v, AllowEmptyStore: %v)",
            activeStore->GetId(),
            reason,
            activeStore->GetDynamicMemoryUsage(),
            activeStore->GetRowCount(),
            activeStore->GetTimestampCount(),
            allowEmptyStore);

        if (activeStore->GetLockCount() > 0) {
            YT_LOG_INFO("Active store is locked and will be kept (StoreId: %v, LockCount: %v)",
                activeStore->GetId(),
                activeStore->GetLockCount());
            YT_VERIFY(LockedStores_.insert(IStorePtr(activeStore)).second);
        } else {
            YT_LOG_INFO("Active store is not locked and will be dropped (StoreId: %v, LockCount: %v)",
                activeStore->GetId(),
                activeStore->GetLockCount());
        }

        OnActiveStoreRotated();
    }

    if (createNewStore) {
        CreateActiveStore();
        if (auto timestamp = Tablet_->GetBackupCheckpointTimestamp()) {
            GetActiveStore()->SetBackupCheckpointTimestamp(timestamp);
        }
    } else {
        ResetActiveStore();
        Tablet_->SetActiveStore(nullptr);
    }

    Tablet_->SetOutOfBandRotationRequested(false);

    StructuredLogger_->OnStoreRotated(activeStore, Tablet_->GetActiveStore());

    YT_LOG_INFO("Tablet stores rotated");
}

bool TStoreManagerBase::IsStoreLocked(IStorePtr store) const
{
    return LockedStores_.find(store) != LockedStores_.end();
}

std::vector<IStorePtr> TStoreManagerBase::GetLockedStores() const
{
    return std::vector<IStorePtr>(LockedStores_.begin(), LockedStores_.end());
}

bool TStoreManagerBase::IsOverflowRotationNeeded() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    const auto* activeStore = GetActiveStore();
    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    auto threshold = mountConfig->DynamicStoreOverflowThreshold;
    return
        activeStore->GetRowCount() >= threshold * mountConfig->MaxDynamicStoreRowCount ||
        activeStore->GetValueCount() >= threshold * mountConfig->MaxDynamicStoreValueCount ||
        activeStore->GetTimestampCount() >= threshold * activeStore->ClampMaxDynamicStoreTimestampCount(mountConfig->MaxDynamicStoreTimestampCount) ||
        activeStore->GetPoolSize() >= threshold * mountConfig->MaxDynamicStorePoolSize;
}

TError TStoreManagerBase::CheckOverflow() const
{
    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    const auto* activeStore = GetActiveStore();
    if (!activeStore) {
        return TError();
    }

    if (activeStore->GetRowCount() >= mountConfig->MaxDynamicStoreRowCount) {
        return TError("Dynamic store row count limit reached")
            << TErrorAttribute("store_id", activeStore->GetId())
            << TErrorAttribute("row_count", activeStore->GetRowCount())
            << TErrorAttribute("row_count_limit", mountConfig->MaxDynamicStoreRowCount);
    }

    if (activeStore->GetValueCount() >= mountConfig->MaxDynamicStoreValueCount) {
        return TError("Dynamic store value count limit reached")
            << TErrorAttribute("store_id", activeStore->GetId())
            << TErrorAttribute("value_count", activeStore->GetValueCount())
            << TErrorAttribute("value_count_limit", mountConfig->MaxDynamicStoreValueCount);
    }

    auto clampedMaxDynamicStoreTimestampCount = activeStore->ClampMaxDynamicStoreTimestampCount(mountConfig->MaxDynamicStoreTimestampCount);

    if (activeStore->GetTimestampCount() >= clampedMaxDynamicStoreTimestampCount) {
        return TError("Dynamic store timestamp count limit reached")
            << TErrorAttribute("store_id", activeStore->GetId())
            << TErrorAttribute("timestamp_count", activeStore->GetTimestampCount())
            << TErrorAttribute("timestamp_count_limit", clampedMaxDynamicStoreTimestampCount)
            << TErrorAttribute("config_timestamp_count_limit", mountConfig->MaxDynamicStoreTimestampCount);
    }

    if (activeStore->GetPoolSize() >= mountConfig->MaxDynamicStorePoolSize) {
        return TError("Dynamic store pool size limit reached")
            << TErrorAttribute("store_id", activeStore->GetId())
            << TErrorAttribute("pool_size", activeStore->GetPoolSize())
            << TErrorAttribute("pool_size_limit", mountConfig->MaxDynamicStorePoolSize);
    }

    return TError();
}

bool TStoreManagerBase::IsRotationPossible() const
{
    if (IsRotationScheduled()) {
        return false;
    }

    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    if (Tablet_->GetOverlappingStoreCount() >= mountConfig->MaxOverlappingStoreCount) {
        return false;
    }

    if (!mountConfig->EnableStoreRotation) {
        return false;
    }

    auto* activeStore = GetActiveStore();
    if (!activeStore) {
        return false;
    }

    // NB: For ordered tablets, we must never attempt to rotate an empty store
    // to avoid collisions of starting row indexes. This check, however, makes
    // sense for sorted tablets as well.
    if (activeStore->GetRowCount() == 0) {
        return false;
    }

    return true;
}

bool TStoreManagerBase::IsForcedRotationPossible() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    // Check for "almost" initial size.
    const auto* activeStore = GetActiveStore();
    if (activeStore->GetPoolCapacity() <= 2 * Config_->PoolChunkSize) {
        return false;
    }

    return true;
}

std::optional<TInstant> TStoreManagerBase::GetLastPeriodicRotationTime() const
{
    return LastPeriodicRotationTime_;
}

void TStoreManagerBase::SetLastPeriodicRotationTime(TInstant value)
{
    LastPeriodicRotationTime_ = value;
}

void TStoreManagerBase::UpdateCommittedStoreRowCount()
{ }

ISortedStoreManagerPtr TStoreManagerBase::AsSorted()
{
    YT_ABORT();
}

IOrderedStoreManagerPtr TStoreManagerBase::AsOrdered()
{
    YT_ABORT();
}

TDynamicStoreId TStoreManagerBase::GenerateDynamicStoreId()
{
    auto doGenerateId = [this] {
        return Tablet_->GenerateId(Tablet_->IsPhysicallySorted()
            ? EObjectType::SortedDynamicTabletStore
            : EObjectType::OrderedDynamicTabletStore);
    };

    if (Tablet_->GetSettings().MountConfig->EnableDynamicStoreRead) {
        if (Tablet_->DynamicStoreIdPool().empty()) {
            // This is an attempt to make the (disaster) scenario when master
            // has not sent dynamic store ids more fail-safe. Ordered tablets
            // require one-to-one correspondence between dynamic stores and
            // their flushed chunks, so we cannot do much: introducing
            // discrepancy here will result in the inconsistent state at master
            // (see CommitUpdateTabletStores). Sorted tables do not have such
            // strict invariants, so we sacrifice the correctness of map-reduce
            // reads and generate the dynamic store that will be not known to
            // the master.

            if (Tablet_->IsPhysicallyOrdered()) {
                YT_LOG_FATAL("Dynamic store id pool is empty, cannot fall back to "
                    "local dynamic store id generation for an ordered tablet");
            }

            auto storeId = doGenerateId();
            YT_LOG_ALERT("Dynamic store id pool is empty, falling back to local "
                "dynamic store id generation. Reads from map-reduce may not see "
                "some recent data (NewStoreId: %v)",
                storeId);

            return storeId;
        }

        return Tablet_->PopDynamicStoreIdFromPool();
    } else {
        return doGenerateId();
    }
}

void TStoreManagerBase::CheckForUnlockedStore(IDynamicStore* store)
{
    if (store == GetActiveStore() || store->GetLockCount() > 0) {
        return;
    }

    YT_LOG_INFO("Store unlocked and will be dropped (StoreId: %v)",
        store->GetId());
    YT_VERIFY(LockedStores_.erase(store) == 1);
}

void TStoreManagerBase::InvalidateCachedChunkReaders()
{
    for (const auto& [storeId, store] : Tablet_->StoreIdMap()) {
        if (store->IsChunk()) {
            store->AsChunk()->InvalidateCachedReaders(Tablet_->GetSettings().StoreReaderConfig);
        }
    }
}

void TStoreManagerBase::UpdateInMemoryMode()
{
    Tablet_->PreloadStoreIds().clear();

    auto mode = GetInMemoryMode();
    for (const auto& [storeId, store] : Tablet_->StoreIdMap()) {
        if (store->IsChunk()) {
            auto chunkStore = store->AsChunk();
            chunkStore->SetInMemoryMode(mode);
            if (chunkStore->GetPreloadState() == EStorePreloadState::Scheduled) {
                chunkStore->UpdatePreloadAttempt(/*isBackoff*/ false);
                Tablet_->PreloadStoreIds().push_back(store->GetId());
                YT_LOG_INFO("Scheduled preload of in-memory store (StoreId: %v)", store->GetId());
            }
        }
    }
}

bool TStoreManagerBase::IsLeader() const
{
    // NB: HydraManager is null in tests.
    return HydraManager_ ? HydraManager_->IsLeader() : false;
}

bool TStoreManagerBase::IsRecovery() const
{
    // NB: HydraManager is null in tests.
    return HydraManager_ ? HydraManager_->IsRecovery() : false;
}

TTimestamp TStoreManagerBase::GenerateMonotonicCommitTimestamp(TTimestamp timestampHint)
{
    auto lastCommitTimestamp = Tablet_->GetLastCommitTimestamp();
    auto monotonicTimestamp = std::max(lastCommitTimestamp + 1, timestampHint);
    Tablet_->UpdateLastCommitTimestamp(monotonicTimestamp);
    return monotonicTimestamp;
}

void TStoreManagerBase::ResetLastPeriodicRotationTime()
{
    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    if (mountConfig->DynamicStoreAutoFlushPeriod && GetActiveStore()) {
        LastPeriodicRotationTime_ = TInstant::Now() - RandomDuration(*mountConfig->DynamicStoreAutoFlushPeriod);
    } else {
        LastPeriodicRotationTime_ = std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
