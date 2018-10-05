#include "store_manager_detail.h"
#include "private.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "store.h"
#include "in_memory_manager.h"
#include "transaction.h"
#include "config.h"

#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/client/table_client/wire_protocol.h>

#include <yt/core/utilex/random.h>

namespace NYT {
namespace NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NHydra;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;

using NTabletNode::NProto::TAddStoreDescriptor;

////////////////////////////////////////////////////////////////////////////////

TStoreManagerBase::TStoreManagerBase(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    ITabletContext* tabletContext,
    NHydra::IHydraManagerPtr hydraManager,
    IInMemoryManagerPtr inMemoryManager,
    NNative::IClientPtr client)
    : Config_(config)
    , Tablet_(tablet)
    , TabletContext_(tabletContext)
    , HydraManager_(std::move(hydraManager))
    , InMemoryManager_(std::move(inMemoryManager))
    , Client_(std::move(client))
    , StoreFlushTag_(NProfiling::TProfileManager::Get()->RegisterTag("method", "store_flush"))
    , Logger(TabletNodeLogger)
{
    Logger.AddTag("TabletId: %v, CellId: %v",
        Tablet_->GetId(),
        TabletContext_->GetCellId());
}

bool TStoreManagerBase::HasActiveLocks() const
{
    const auto* activeStore = GetActiveStore();
    if (activeStore->GetLockCount() > 0) {
        return true;
    }

    if (!LockedStores_.empty()) {
        return true;
    }

    return false;
}

bool TStoreManagerBase::HasUnflushedStores() const
{
    for (const auto& pair : Tablet_->StoreIdMap()) {
        const auto& store = pair.second;
        auto state = store->GetStoreState();
        // NB: When the table is being frozen we don't flush empty dynamic stores.
        if (state != EStoreState::Persistent &&
            !(state == EStoreState::ActiveDynamic &&
              Tablet_->GetState() == ETabletState::FreezeFlushing &&
              store->AsDynamic()->GetRowCount() == 0))
        {
            return true;
        }
    }
    return false;
}

void TStoreManagerBase::StartEpoch(TTabletSlotPtr slot)
{
    Tablet_->StartEpoch(slot);

    InitializeRotation();

    UpdateInMemoryMode();
}

void TStoreManagerBase::StopEpoch()
{
    Tablet_->StopEpoch();

    for (const auto& pair : Tablet_->StoreIdMap()) {
        const auto& store = pair.second;
        if (store->IsDynamic()) {
            store->AsDynamic()->SetFlushState(EStoreFlushState::None);
        }
        if (store->IsChunk()) {
            auto chunkStore = store->AsChunk();
            chunkStore->SetCompactionState(EStoreCompactionState::None);

            if (chunkStore->GetPreloadState() == EStorePreloadState::Scheduled ||
                chunkStore->GetPreloadState() == EStorePreloadState::Running)
            {
                // Running preloads are cancelled in cancellable invoker. There are no
                // concurrent preloads when this code is running, because execution is
                // serialized in one thread.
                chunkStore->SetPreloadState(EStorePreloadState::None);
            }
        }
    }

    Tablet_->PreloadStoreIds().clear();
}

void TStoreManagerBase::InitializeRotation()
{
    const auto& config = Tablet_->GetConfig();
    if (config->DynamicStoreAutoFlushPeriod) {
        LastRotated_ = TInstant::Now() - RandomDuration(*config->DynamicStoreAutoFlushPeriod);
    }

    RotationScheduled_ = false;
}

bool TStoreManagerBase::IsRotationScheduled() const
{
    return RotationScheduled_;
}

void TStoreManagerBase::ScheduleRotation()
{
    if (RotationScheduled_)
        return;

    RotationScheduled_ = true;

    LOG_INFO("Tablet store rotation scheduled");
}

void TStoreManagerBase::AddStore(IStorePtr store, bool onMount)
{
    Tablet_->AddStore(store);

    if (onMount) {
        // After mount preload will be performed in StartEpoch
        return;
    }

    if (store->IsChunk()) {
        auto chunkStore = store->AsChunk();
        if (chunkStore->GetPreloadState() == EStorePreloadState::Scheduled) {
            auto chunkData = InMemoryManager_->EvictInterceptedChunkData(chunkStore->GetId());
            if (!TryPreloadStoreFromInterceptedData(chunkStore, chunkData)) {
                Tablet_->PreloadStoreIds().push_back(store->GetId());
                LOG_INFO_UNLESS(IsRecovery(), "Scheduled preload of in-memory store (StoreId: %v)", store->GetId());
            }
        }
    }
}

void TStoreManagerBase::RemoveStore(IStorePtr store)
{
    Y_ASSERT(store->GetStoreState() != EStoreState::ActiveDynamic);

    store->SetStoreState(EStoreState::Removed);
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
            }
            break;
        }
        case EStoreType::SortedChunk:
        case EStoreType::OrderedChunk: {
            auto chunkStore = store->AsChunk();
            auto compactionState = chunkStore->GetCompactionState();
            if (compactionState == EStoreCompactionState::Complete) {
                chunkStore->SetCompactionState(EStoreCompactionState::None);
                chunkStore->UpdateCompactionAttempt();
            }
            break;
        }
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

    return true;
}

TStoreFlushCallback TStoreManagerBase::BeginStoreFlush(
    IDynamicStorePtr store,
    TTabletSnapshotPtr tabletSnapshot,
    bool isUnmountWorkflow)
{
    YCHECK(store->GetFlushState() == EStoreFlushState::None);
    store->SetFlushState(EStoreFlushState::Running);
    return MakeStoreFlushCallback(store, tabletSnapshot, isUnmountWorkflow);
}

void TStoreManagerBase::EndStoreFlush(IDynamicStorePtr store)
{
    YCHECK(store->GetFlushState() == EStoreFlushState::Running);
    store->SetFlushState(EStoreFlushState::Complete);
}

void TStoreManagerBase::BackoffStoreFlush(IDynamicStorePtr store)
{
    YCHECK(store->GetFlushState() == EStoreFlushState::Running);
    store->SetFlushState(EStoreFlushState::None);
    store->UpdateFlushAttemptTimestamp();
}

void TStoreManagerBase::BeginStoreCompaction(IChunkStorePtr store)
{
    YCHECK(store->GetCompactionState() == EStoreCompactionState::None);
    store->SetCompactionState(EStoreCompactionState::Running);
}

void TStoreManagerBase::EndStoreCompaction(IChunkStorePtr store)
{
    YCHECK(store->GetCompactionState() == EStoreCompactionState::Running);
    store->SetCompactionState(EStoreCompactionState::Complete);
}

void TStoreManagerBase::BackoffStoreCompaction(IChunkStorePtr store)
{
    YCHECK(store->GetCompactionState() == EStoreCompactionState::Running);
    store->SetCompactionState(EStoreCompactionState::None);
    store->UpdateCompactionAttempt();
}

bool TStoreManagerBase::TryPreloadStoreFromInterceptedData(
    IChunkStorePtr store,
    TInMemoryChunkDataPtr chunkData)
{
    if (!chunkData) {
        LOG_WARNING_UNLESS(
            IsRecovery(),
            "Intercepted chunk data for in-memory store is missing (StoreId: %v)",
            store->GetId());
        return false;
    }

    if(!chunkData->Finalized) {
        LOG_WARNING_UNLESS(
            IsRecovery(),
            "Intercepted chunk data for in-memory store is not finalized (StoreId: %v)",
            store->GetId());
        return false;
    }

    auto mode = Tablet_->GetConfig()->InMemoryMode;
    if (mode != chunkData->InMemoryMode) {
        LOG_WARNING_UNLESS(
            IsRecovery(),
            "Intercepted chunk data for in-memory store has invalid mode (StoreId: %v, ExpectedMode: %v, ActualMode: %v)",
            store->GetId(),
            mode,
            chunkData->InMemoryMode);
        return false;
    }

    store->Preload(chunkData);
    store->SetPreloadState(EStorePreloadState::Complete);

    LOG_INFO_UNLESS(IsRecovery(), "In-memory store preloaded from intercepted chunk data (StoreId: %v, Mode: %v)",
        store->GetId(),
        mode);

    return true;
}

IChunkStorePtr TStoreManagerBase::PeekStoreForPreload()
{
    LOG_INFO("Peeking store for preload");

    for (size_t size = Tablet_->PreloadStoreIds().size(); size != 0; --size) {
        auto id = Tablet_->PreloadStoreIds().front();
        auto store = Tablet_->FindStore(id);
        if (store) {
            auto chunkStore = store->AsChunk();
            if (chunkStore->GetPreloadState() == EStorePreloadState::Scheduled) {
                if (chunkStore->IsPreloadAllowed()) {
                    LOG_INFO("Peeked store for preload (StoreId: %v)", chunkStore->GetId());
                    return chunkStore;
                } else {
                    LOG_INFO("Preload not allowed (StoreId: %v)", chunkStore->GetId());
                }
                Tablet_->PreloadStoreIds().pop_front();
                Tablet_->PreloadStoreIds().push_back(id);
                continue;
            } else {
                LOG_INFO("Preload not scheduled (StoreId: %v)", chunkStore->GetId());
            }
        }
        Tablet_->PreloadStoreIds().pop_front();
    }
    return nullptr;
}

void TStoreManagerBase::BeginStorePreload(IChunkStorePtr store, TCallback<TFuture<void>()> callbackFuture)
{
    YCHECK(store->GetId() == Tablet_->PreloadStoreIds().front());
    Tablet_->PreloadStoreIds().pop_front();

    YCHECK(store->GetPreloadState() == EStorePreloadState::Scheduled);
    store->SetPreloadState(EStorePreloadState::Running);
    store->SetPreloadFuture(callbackFuture.Run());
}

void TStoreManagerBase::EndStorePreload(IChunkStorePtr store)
{
    YCHECK(store->GetPreloadState() == EStorePreloadState::Running);
    store->SetPreloadState(EStorePreloadState::Complete);
    store->SetPreloadFuture(TFuture<void>());
}

void TStoreManagerBase::BackoffStorePreload(IChunkStorePtr store)
{
    std::vector<IInvokerPtr> feasibleInvokers{
        Tablet_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Default),
        Tablet_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Mutation)};
    VERIFY_INVOKERS_AFFINITY(feasibleInvokers);

    YCHECK(store->GetPreloadState() == EStorePreloadState::Running);

    store->SetPreloadFuture(TFuture<void>());
    store->SetPreloadState(EStorePreloadState::Scheduled);
    Tablet_->PreloadStoreIds().push_back(store->GetId());
}

EInMemoryMode TStoreManagerBase::GetInMemoryMode() const
{
    return Tablet_->GetConfig()->InMemoryMode;
}

void TStoreManagerBase::Mount(const std::vector<TAddStoreDescriptor>& storeDescriptors)
{
    for (const auto& descriptor : storeDescriptors) {
        auto type = EStoreType(descriptor.store_type());
        auto storeId = FromProto<TChunkId>(descriptor.store_id());
        YCHECK(descriptor.has_chunk_meta());
        YCHECK(!descriptor.has_backing_store_id());
        auto store = TabletContext_->CreateStore(
            Tablet_,
            type,
            storeId,
            &descriptor);
        AddStore(store->AsChunk(), true);

        const auto& extensions = descriptor.chunk_meta().extensions();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(extensions);
        if (miscExt.has_max_timestamp()) {
            Tablet_->UpdateLastCommitTimestamp(miscExt.max_timestamp());
        }
    }

    // NB: Active store must be created _after_ chunk stores to make sure it receives
    // the right starting row index (for ordered tablets only).
    CreateActiveStore();

    Tablet_->SetState(ETabletState::Mounted);
}

void TStoreManagerBase::Remount(
    TTableMountConfigPtr mountConfig,
    TTabletChunkReaderConfigPtr readerConfig,
    TTabletChunkWriterConfigPtr writerConfig,
    TTabletWriterOptionsPtr writerOptions)
{
    Tablet_->SetConfig(mountConfig);
    Tablet_->SetReaderConfig(readerConfig);
    Tablet_->SetWriterConfig(writerConfig);
    Tablet_->SetWriterOptions(writerOptions);

    UpdateInMemoryMode();
}

void TStoreManagerBase::Rotate(bool createNewStore)
{
    RotationScheduled_ = false;
    LastRotated_ = TInstant::Now();

    auto* activeStore = GetActiveStore();
    YCHECK(activeStore);
    activeStore->SetStoreState(EStoreState::PassiveDynamic);

    if (activeStore->GetLockCount() > 0) {
        LOG_INFO_UNLESS(IsRecovery(), "Active store is locked and will be kept (StoreId: %v, LockCount: %v)",
            activeStore->GetId(),
            activeStore->GetLockCount());
        YCHECK(LockedStores_.insert(IStorePtr(activeStore)).second);
    } else {
        LOG_INFO_UNLESS(IsRecovery(), "Active store is not locked and will be dropped (StoreId: %v)",
            activeStore->GetId(),
            activeStore->GetLockCount());
    }

    OnActiveStoreRotated();

    if (createNewStore) {
        CreateActiveStore();
    } else {
        ResetActiveStore();
        Tablet_->SetActiveStore(nullptr);
    }

    LOG_INFO_UNLESS(IsRecovery(), "Tablet stores rotated");
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
    const auto& config = Tablet_->GetConfig();
    auto threshold = config->DynamicStoreOverflowThreshold;
    return
        activeStore->GetRowCount() >= threshold * config->MaxDynamicStoreRowCount ||
        activeStore->GetValueCount() >= threshold * config->MaxDynamicStoreValueCount ||
        activeStore->GetTimestampCount() >= threshold * config->MaxDynamicStoreTimestampCount ||
        activeStore->GetPoolSize() >= threshold * config->MaxDynamicStorePoolSize;
}

TError TStoreManagerBase::CheckOverflow() const
{
    const auto& config = Tablet_->GetConfig();
    const auto* activeStore = GetActiveStore();
    if (!activeStore) {
        return TError();
    }

    if (activeStore->GetRowCount() >= config->MaxDynamicStoreRowCount) {
        return TError("Dynamic store row count limit reached")
            << TErrorAttribute("store_id", activeStore->GetId())
            << TErrorAttribute("row_count", activeStore->GetRowCount())
            << TErrorAttribute("row_count_limit", config->MaxDynamicStoreRowCount);
    }

    if (activeStore->GetValueCount() >= config->MaxDynamicStoreValueCount) {
        return TError("Dynamic store value count limit reached")
            << TErrorAttribute("store_id", activeStore->GetId())
            << TErrorAttribute("value_count", activeStore->GetValueCount())
            << TErrorAttribute("value_count_limit", config->MaxDynamicStoreValueCount);
    }

    if (activeStore->GetTimestampCount() >= config->MaxDynamicStoreTimestampCount) {
        return TError("Dynamic store timestamp count limit reached")
            << TErrorAttribute("store_id", activeStore->GetId())
            << TErrorAttribute("timestamp_count", activeStore->GetTimestampCount())
            << TErrorAttribute("timestamp_count_limit", config->MaxDynamicStoreTimestampCount);
    }

    if (activeStore->GetPoolSize() >= config->MaxDynamicStorePoolSize) {
        return TError("Dynamic store pool size limit reached")
            << TErrorAttribute("store_id", activeStore->GetId())
            << TErrorAttribute("pool_size", activeStore->GetPoolSize())
            << TErrorAttribute("pool_size_limit", config->MaxDynamicStorePoolSize);
    }

    return TError();
}

bool TStoreManagerBase::IsPeriodicRotationNeeded() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    const auto* activeStore = GetActiveStore();
    const auto& config = Tablet_->GetConfig();
    return
        config->DynamicStoreAutoFlushPeriod  &&
        TInstant::Now() > LastRotated_ + *config->DynamicStoreAutoFlushPeriod &&
        activeStore->GetRowCount() > 0;
}

bool TStoreManagerBase::IsRotationPossible() const
{
    if (IsRotationScheduled()) {
        return false;
    }

    if (Tablet_->GetOverlappingStoreCount() >= Tablet_->GetConfig()->MaxOverlappingStoreCount) {
        return false;
    }

    if (!Tablet_->GetConfig()->EnableCompactionAndPartitioning) {
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

ISortedStoreManagerPtr TStoreManagerBase::AsSorted()
{
    Y_UNREACHABLE();
}

IOrderedStoreManagerPtr TStoreManagerBase::AsOrdered()
{
    Y_UNREACHABLE();
}

void TStoreManagerBase::CheckForUnlockedStore(IDynamicStore* store)
{
    if (store == GetActiveStore() || store->GetLockCount() > 0) {
        return;
    }

    LOG_INFO_UNLESS(IsRecovery(), "Store unlocked and will be dropped (StoreId: %v)",
        store->GetId());
    YCHECK(LockedStores_.erase(store) == 1);
}

void TStoreManagerBase::UpdateInMemoryMode()
{
    Tablet_->PreloadStoreIds().clear();

    auto mode = GetInMemoryMode();
    for (const auto& pair : Tablet_->StoreIdMap()) {
        const auto& store = pair.second;
        if (store->IsChunk()) {
            auto chunkStore = store->AsChunk();
            chunkStore->SetInMemoryMode(mode);
            if (chunkStore->GetPreloadState() == EStorePreloadState::Scheduled) {
                chunkStore->UpdatePreloadAttempt(false);
                Tablet_->PreloadStoreIds().push_back(store->GetId());
                LOG_INFO_UNLESS(IsRecovery(), "Scheduled preload of in-memory store (StoreId: %v)", store->GetId());
            }
        }
    }
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

