#include "store_manager_detail.h"
#include "private.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "store.h"
#include "in_memory_manager.h"
#include "config.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TStoreManagerBase::TStoreManagerBase(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    ITabletContext* tabletContext,
    NHydra::IHydraManagerPtr hydraManager,
    TInMemoryManagerPtr inMemoryManager)
    : Config_(config)
    , Tablet_(tablet)
    , TabletContext_(tabletContext)
    , HydraManager_(hydraManager)
    , InMemoryManager_(inMemoryManager)
    , Logger(TabletNodeLogger)
{
    YCHECK(Config_);
    YCHECK(Tablet_);

    Logger.AddTag("TabletId: %v, CellId: %v",
        Tablet_->GetId(),
        TabletContext_->GetCellId());
}

TTablet* TStoreManagerBase::GetTablet() const
{
    return Tablet_;
}

bool TStoreManagerBase::HasUnflushedStores() const
{
    for (const auto& pair : Tablet_->Stores()) {
        const auto& store = pair.second;
        auto state = store->GetStoreState();
        if (state != EStoreState::Persistent) {
            return true;
        }
    }
    return false;
}

void TStoreManagerBase::StartEpoch(TTabletSlotPtr slot)
{
    Tablet_->StartEpoch(slot);

    const auto& config = Tablet_->GetConfig();
    LastRotated_ = TInstant::Now() - RandomDuration(config->DynamicStoreAutoFlushPeriod);

    RotationScheduled_ = false;
}

void TStoreManagerBase::StopEpoch()
{
    Tablet_->StopEpoch();

    for (const auto& pair : Tablet_->Stores()) {
        const auto& store = pair.second;
        if (store->IsDynamic()) {
            store->AsDynamic()->SetFlushState(EStoreFlushState::None);
        }
        if (store->IsChunk()) {
            store->AsChunk()->SetCompactionState(EStoreCompactionState::None);
        }
    }
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

    if (InMemoryManager_ &&
        store->IsChunk() &&
        Tablet_->GetConfig()->InMemoryMode != EInMemoryMode::None)
    {
        auto chunkStore = store->AsChunk();
        TInMemoryChunkDataPtr chunkData;
        if (!onMount) {
            chunkData = InMemoryManager_->EvictInterceptedChunkData(chunkStore->GetId());
        }
        if (!chunkData || !TryPreloadStoreFromInterceptedData(chunkStore, chunkData)) {
            ScheduleStorePreload(chunkStore);
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
    NConcurrency::TDelayedExecutor::Submit(
        BIND([=] () {
            switch (store->GetType()) {
                case EStoreType::SortedDynamic: {
                    auto dynamicStore = store->AsDynamic();
                    if (dynamicStore->GetFlushState() == EStoreFlushState::Complete) {
                        dynamicStore->SetFlushState(EStoreFlushState::None);
                    }
                    break;
                }
                case EStoreType::SortedChunk: {
                    auto chunkStore = store->AsChunk();
                    if (chunkStore->GetCompactionState() == EStoreCompactionState::Complete) {
                        chunkStore->SetCompactionState(EStoreCompactionState::None);
                    }
                    break;
                }
            }
        }).Via(Tablet_->GetEpochAutomatonInvoker()),
        Config_->ErrorBackoffTime);
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

    return true;
}

void TStoreManagerBase::BeginStoreFlush(IDynamicStorePtr store)
{
    YCHECK(store->GetFlushState() == EStoreFlushState::None);
    store->SetFlushState(EStoreFlushState::Running);
}

void TStoreManagerBase::EndStoreFlush(IDynamicStorePtr store)
{
    YCHECK(store->GetFlushState() == EStoreFlushState::Running);
    store->SetFlushState(EStoreFlushState::Complete);
}

void TStoreManagerBase::BackoffStoreFlush(IDynamicStorePtr store)
{
    YCHECK(store->GetFlushState() == EStoreFlushState::Running);
    store->SetFlushState(EStoreFlushState::Failed);
    NConcurrency::TDelayedExecutor::Submit(
        BIND([store] () {
            YCHECK(store->GetFlushState() == EStoreFlushState::Failed);
            store->SetFlushState(EStoreFlushState::None);
        }).Via(Tablet_->GetEpochAutomatonInvoker()),
        Config_->ErrorBackoffTime);
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
    store->SetCompactionState(EStoreCompactionState::Failed);
    NConcurrency::TDelayedExecutor::Submit(
        BIND([store] () {
            YCHECK(store->GetCompactionState() == EStoreCompactionState::Failed);
            store->SetCompactionState(EStoreCompactionState::None);
        }).Via(Tablet_->GetEpochAutomatonInvoker()),
        Config_->ErrorBackoffTime);
}

void TStoreManagerBase::ScheduleStorePreload(IChunkStorePtr store)
{
    auto state = store->GetPreloadState();
    YCHECK(state != EStorePreloadState::Disabled);

    if (state != EStorePreloadState::None && state != EStorePreloadState::Failed) {
        return;
    }

    Tablet_->PreloadStoreIds().push_back(store->GetId());
    store->SetPreloadState(EStorePreloadState::Scheduled);

    LOG_INFO("Scheduled preload of in-memory store (StoreId: %v, Mode: %v)",
        store->GetId(),
        Tablet_->GetConfig()->InMemoryMode);
}

bool TStoreManagerBase::TryPreloadStoreFromInterceptedData(
    IChunkStorePtr store,
    TInMemoryChunkDataPtr chunkData)
{
    YCHECK(store);
    YCHECK(chunkData);

    auto state = store->GetPreloadState();
    YCHECK(state == EStorePreloadState::None);

    auto mode = Tablet_->GetConfig()->InMemoryMode;
    YCHECK(mode != EInMemoryMode::None);

    if (mode != chunkData->InMemoryMode) {
        LOG_WARNING("Intercepted chunk data for in-memory store has invalid mode (StoreId: %v, ExpectedMode: %v, ActualMode: %v)",
            store->GetId(),
            mode,
            chunkData->InMemoryMode);
        return false;
    }

    store->Preload(chunkData);
    store->SetPreloadState(EStorePreloadState::Complete);

    LOG_INFO("In-memory store preloaded from intercepted chunk data (StoreId: %v, Mode: %v)",
        store->GetId(),
        mode);

    return true;
}

IChunkStorePtr TStoreManagerBase::PeekStoreForPreload()
{
    while (!Tablet_->PreloadStoreIds().empty()) {
        auto id = Tablet_->PreloadStoreIds().front();
        auto store = Tablet_->FindStore(id);
        if (store) {
            auto chunkStore = store->AsChunk();
            if (chunkStore->GetPreloadState() == EStorePreloadState::Scheduled) {
                return chunkStore;
            }
        }
        Tablet_->PreloadStoreIds().pop_front();
    }
    return nullptr;
}

void TStoreManagerBase::BeginStorePreload(IChunkStorePtr store, TFuture<void> future)
{
    YCHECK(store->GetId() == Tablet_->PreloadStoreIds().front());
    Tablet_->PreloadStoreIds().pop_front();
    store->SetPreloadState(EStorePreloadState::Running);
    store->SetPreloadFuture(future);
}

void TStoreManagerBase::EndStorePreload(IChunkStorePtr store)
{
    store->SetPreloadState(EStorePreloadState::Complete);
    store->SetPreloadFuture(TFuture<void>());
}

void TStoreManagerBase::BackoffStorePreload(IChunkStorePtr store)
{
    if (store->GetPreloadState() != EStorePreloadState::Running) {
        return;
    }

    store->SetPreloadState(EStorePreloadState::Failed);
    store->SetPreloadFuture(TFuture<void>());
    NConcurrency::TDelayedExecutor::Submit(
        BIND([=] () {
            if (store->GetPreloadState() == EStorePreloadState::Failed) {
                ScheduleStorePreload(store);
            }
        }).Via(Tablet_->GetEpochAutomatonInvoker()),
        Config_->ErrorBackoffTime);
}

void TStoreManagerBase::Remount(TTableMountConfigPtr mountConfig, TTabletWriterOptionsPtr writerOptions)
{
    Tablet_->SetConfig(mountConfig);
    Tablet_->SetWriterOptions(writerOptions);

    UpdateInMemoryMode();
}

void TStoreManagerBase::UpdateInMemoryMode()
{
    auto mode = Tablet_->GetConfig()->InMemoryMode;

    Tablet_->PreloadStoreIds().clear();

    for (const auto& pair : Tablet_->Stores()) {
        const auto& store = pair.second;
        if (store->IsChunk()) {
            auto chunkStore = store->AsChunk();
            chunkStore->SetInMemoryMode(mode);
            if (mode != EInMemoryMode::None) {
                ScheduleStorePreload(chunkStore);
            }
        }
    }
}

bool TStoreManagerBase::IsRecovery() const
{
    // NB: HydraManager is null in tests.
    return HydraManager_ ? HydraManager_->IsRecovery() : false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

