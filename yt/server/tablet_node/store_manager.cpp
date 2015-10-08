#include "stdafx.h"
#include "store_manager.h"
#include "tablet.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"
#include "transaction.h"
#include "config.h"
#include "tablet_slot.h"
#include "transaction_manager.h"
#include "in_memory_manager.h"
#include "private.h"

#include <core/misc/small_vector.h>

#include <core/concurrency/scheduler.h>

#include <ytlib/object_client/public.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/schemaful_reader.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/helpers.h>

#include <ytlib/chunk_client/block_cache.h>

#include <server/hydra/hydra_manager.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NObjectClient;

using NVersionedTableClient::TKey;

////////////////////////////////////////////////////////////////////////////////

TStoreManager::TStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    TCallback<TDynamicMemoryStorePtr()> dynamicMemoryStoreFactory)
    : Config_(config)
    , Tablet_(tablet)
    , DynamicMemoryStoreFactory_(dynamicMemoryStoreFactory)
    , KeyColumnCount_(Tablet_->GetKeyColumnCount())
    , Logger(TabletNodeLogger)
{
    YCHECK(Config_);
    YCHECK(Tablet_);
    YCHECK(DynamicMemoryStoreFactory_);

    Logger.AddTag("TabletId: %v", Tablet_->GetTabletId());
    if (Tablet_->GetSlot()) {
        Logger.AddTag("CellId: %v", Tablet_->GetSlot()->GetCellId());
    }

    for (const auto& pair : Tablet_->Stores()) {
        const auto& store = pair.second;
        if (store->GetStoreState() != EStoreState::ActiveDynamic) {
            MaxTimestampToStore_.insert(std::make_pair(store->GetMaxTimestamp(), store));
        }
    }

    // This schedules preload of in-memory tablets.
    UpdateInMemoryMode();
}

TTablet* TStoreManager::GetTablet() const
{
    return Tablet_;
}

bool TStoreManager::HasActiveLocks() const
{
    if (Tablet_->GetActiveStore()->GetLockCount() > 0) {
        return true;
    }
   
    if (!LockedStores_.empty()) {
        return true;
    }

    return false;
}

bool TStoreManager::HasUnflushedStores() const
{
    for (const auto& pair : Tablet_->Stores()) {
        const auto& store = pair.second;
        // COMPAT(sandello): Temporary treat "remove_committing" chunk stores as flushed.
        if (store->GetType() != EStoreType::Chunk) {
            return true;
        }
        if (store->GetStoreState() != EStoreState::Persistent &&
            store->GetStoreState() != EStoreState::RemoveCommitting) {
            return true;
        }
    }
    return false;
}

void TStoreManager::StartEpoch(TTabletSlotPtr slot)
{
    Tablet_->StartEpoch(slot);
    const auto& config = Tablet_->GetConfig();
    LastRotated_ = TInstant::Now() - RandomDuration(config->MemoryStoreAutoFlushPeriod);
    RotationScheduled_ = false;
}

void TStoreManager::StopEpoch()
{
    Tablet_->StopEpoch();

    for (const auto& pair : Tablet_->Stores()) {
        const auto& store = pair.second;
        switch (store->GetType()) {
            case EStoreType::DynamicMemory: {
                auto dynamicStore = store->AsDynamicMemory();
                dynamicStore->SetFlushState(EStoreFlushState::None);
                break;
            }
            case EStoreType::Chunk: {
                auto chunkStore = store->AsChunk();
                chunkStore->SetCompactionState(EStoreCompactionState::None);
                break;
            }
        }
    }
}

TDynamicRowRef TStoreManager::WriteRowAtomic(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prelock)
{
    ValidateServerDataRow(row, KeyColumnCount_, Tablet_->Schema());

    YASSERT(row.GetCount() >= KeyColumnCount_);
    if (row.GetCount() == KeyColumnCount_) {
        THROW_ERROR_EXCEPTION("Empty writes are not allowed")
            << TErrorAttribute("transaction_id", transaction->GetId())
            << TErrorAttribute("tablet_id", Tablet_->GetTabletId())
            << TErrorAttribute("key", row);
    }

    ui32 lockMask = ComputeLockMask(row);

    if (prelock) {
        CheckInactiveStoresLocks(
            transaction,
            row,
            lockMask);
    }

    const auto& store = Tablet_->GetActiveStore();
    auto dynamicRow = store->WriteRowAtomic(
        transaction,
        row,
        prelock,
        lockMask);
    return TDynamicRowRef(store.Get(), dynamicRow);
}

void TStoreManager::WriteRowNonAtomic(
    const TTransactionId& transactionId,
    TTimestamp commitTimestamp,
    TUnversionedRow row)
{
    ValidateServerDataRow(row, KeyColumnCount_, Tablet_->Schema());

    YASSERT(row.GetCount() >= KeyColumnCount_);
    if (row.GetCount() == KeyColumnCount_) {
        THROW_ERROR_EXCEPTION("Empty writes are not allowed")
            << TErrorAttribute("transaction_id", transactionId)
            << TErrorAttribute("tablet_id", Tablet_->GetTabletId())
            << TErrorAttribute("key", row);
    }

    const auto& store = Tablet_->GetActiveStore();
    store->WriteRowNonAtomic(row, commitTimestamp);
}

TDynamicRowRef TStoreManager::DeleteRowAtomic(
    TTransaction* transaction,
    NVersionedTableClient::TKey key,
    bool prelock)
{
    ValidateServerKey(key, KeyColumnCount_, Tablet_->Schema());

    if (prelock) {
        CheckInactiveStoresLocks(
            transaction,
            key,
            TDynamicRow::PrimaryLockMask);
    }

    const auto& store = Tablet_->GetActiveStore();
    auto dynamicRow = store->DeleteRowAtomic(
        transaction,
        key,
        prelock);
    return TDynamicRowRef(store.Get(), dynamicRow);
}

void TStoreManager::DeleteRowNonAtomic(
    const TTransactionId& transactionId,
    TTimestamp commitTimestamp,
    NVersionedTableClient::TKey key)
{
    ValidateServerKey(key, KeyColumnCount_, Tablet_->Schema());

    const auto& store = Tablet_->GetActiveStore();
    store->DeleteRowNonAtomic(key, commitTimestamp);
}

void TStoreManager::ConfirmRow(TTransaction* transaction, const TDynamicRowRef& rowRef)
{
    rowRef.Store->ConfirmRow(transaction, rowRef.Row);
}

void TStoreManager::PrepareRow(TTransaction* transaction, const TDynamicRowRef& rowRef)
{
    rowRef.Store->PrepareRow(transaction, rowRef.Row);
}

void TStoreManager::CommitRow(TTransaction* transaction, const TDynamicRowRef& rowRef)
{
    const auto& activeStore = Tablet_->GetActiveStore();
    if (rowRef.Store == activeStore) {
        activeStore->CommitRow(transaction, rowRef.Row);
    } else {
        auto migratedRow = activeStore->MigrateRow(transaction, rowRef.Row);
        rowRef.Store->CommitRow(transaction, rowRef.Row);
        CheckForUnlockedStore(rowRef.Store);
        activeStore->CommitRow(transaction, migratedRow);
    }
}

void TStoreManager::AbortRow(TTransaction* transaction, const TDynamicRowRef& rowRef)
{
    rowRef.Store->AbortRow(transaction, rowRef.Row);
    CheckForUnlockedStore(rowRef.Store);
}

ui32 TStoreManager::ComputeLockMask(TUnversionedRow row)
{
    const auto& columnIndexToLockIndex = Tablet_->ColumnIndexToLockIndex();
    ui32 lockMask = 0;
    for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
        const auto& value = row[index];
        int lockIndex = columnIndexToLockIndex[value.Id];
        lockMask |= (1 << lockIndex);
    }
    YASSERT(lockMask != 0);
    return lockMask;
}

void TStoreManager::CheckInactiveStoresLocks(
    TTransaction* transaction,
    TUnversionedRow row,
    ui32 lockMask)
{
    for (const auto& store : LockedStores_) {
        store->CheckRowLocks(
            row,
            transaction,
            lockMask);
    }

    for (auto it = MaxTimestampToStore_.rbegin();
         it != MaxTimestampToStore_.rend() && it->first > transaction->GetStartTimestamp();
         ++it)
    {
        const auto& store = it->second;
        // Avoid checking locked stores twice.
        if (store->GetType() == EStoreType::DynamicMemory &&
            store->AsDynamicMemory()->GetLockCount() > 0)
            continue;
        store->CheckRowLocks(
            row,
            transaction,
            lockMask);
    }
}

void TStoreManager::CheckForUnlockedStore(TDynamicMemoryStore* store)
{
    if (store == Tablet_->GetActiveStore() || store->GetLockCount() > 0)
        return;

    LOG_INFO_UNLESS(IsRecovery(), "Store unlocked and will be dropped (StoreId: %v)",
        store->GetId());
    YCHECK(LockedStores_.erase(store) == 1);
}

bool TStoreManager::IsOverflowRotationNeeded() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    const auto& store = Tablet_->GetActiveStore();
    const auto& config = Tablet_->GetConfig();
    return
        store->GetKeyCount() >= config->MaxMemoryStoreKeyCount ||
        store->GetValueCount() >= config->MaxMemoryStoreValueCount ||
        store->GetPoolCapacity() >= config->MaxMemoryStorePoolSize;
}

bool TStoreManager::IsPeriodicRotationNeeded() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    const auto& store = Tablet_->GetActiveStore();
    const auto& config = Tablet_->GetConfig();
    return
        TInstant::Now() > LastRotated_ + config->MemoryStoreAutoFlushPeriod &&
        store->GetKeyCount() > 0;
}

bool TStoreManager::IsRotationPossible() const
{
    if (IsRotationScheduled()) {
        return false;
    }

    if (!Tablet_->GetActiveStore()) {
        return false;
    }

    return true;
}

bool TStoreManager::IsForcedRotationPossible() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    const auto& store = Tablet_->GetActiveStore();
    // Check for "almost" initial size.
    if (store->GetPoolCapacity() <=  2 * Config_->PoolChunkSize) {
        return false;
    }

    return true;
}

bool TStoreManager::IsRotationScheduled() const
{
    return RotationScheduled_;
}

void TStoreManager::ScheduleRotation()
{
    if (RotationScheduled_)
        return;
    
    RotationScheduled_ = true;

    LOG_INFO("Tablet store rotation scheduled");
}

void TStoreManager::Rotate(bool createNewStore)
{
    RotationScheduled_ = false;
    LastRotated_ = TInstant::Now();

    const auto& store = Tablet_->GetActiveStore();
    YCHECK(store);

    store->SetStoreState(EStoreState::PassiveDynamic);

    if (store->GetLockCount() > 0) {
        LOG_INFO_UNLESS(IsRecovery(), "Active store is locked and will be kept (StoreId: %v, LockCount: %v)",
            store->GetId(),
            store->GetLockCount());
        YCHECK(LockedStores_.insert(store).second);
    } else {
        LOG_INFO_UNLESS(IsRecovery(), "Active store is not locked and will be dropped (StoreId: %v)",
            store->GetId(),
            store->GetLockCount());
    }

    MaxTimestampToStore_.insert(std::make_pair(store->GetMaxTimestamp(), store));

    if (createNewStore) {
        CreateActiveStore();
    } else {
        Tablet_->SetActiveStore(nullptr);
    }

    LOG_INFO_UNLESS(IsRecovery(), "Tablet stores rotated");
}

void TStoreManager::AddStore(IStorePtr store)
{
    Tablet_->AddStore(store);
    MaxTimestampToStore_.insert(std::make_pair(store->GetMaxTimestamp(), store));
}

void TStoreManager::RemoveStore(IStorePtr store)
{
    YASSERT(store->GetStoreState() != EStoreState::ActiveDynamic);

    store->SetStoreState(EStoreState::Removed);
    Tablet_->RemoveStore(store);

    // The range is likely to contain at most one element.
    auto range = MaxTimestampToStore_.equal_range(store->GetMaxTimestamp());
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second == store) {
            MaxTimestampToStore_.erase(it);
            break;
        }
    }
}

void TStoreManager::CreateActiveStore()
{
    auto store = DynamicMemoryStoreFactory_.Run();
    Tablet_->AddStore(store);
    Tablet_->SetActiveStore(store);

    LOG_INFO_UNLESS(IsRecovery(), "Active store created (StoreId: %v)",
        store->GetId());
}

bool TStoreManager::IsStoreLocked(TDynamicMemoryStorePtr store) const
{
    return LockedStores_.find(store) != LockedStores_.end();
}

const yhash_set<TDynamicMemoryStorePtr>& TStoreManager::GetLockedStores() const
{
    return LockedStores_;
}

bool TStoreManager::IsStoreFlushable(IStorePtr store)
{
    if (store->GetStoreState() != EStoreState::PassiveDynamic) {
        return false;
    }

    auto dynamicStore = store->AsDynamicMemory();

    if (dynamicStore->GetFlushState() != EStoreFlushState::None) {
        return false;
    }

    return true;
}

void TStoreManager::BeginStoreFlush(TDynamicMemoryStorePtr store)
{
    YCHECK(store->GetFlushState() == EStoreFlushState::None);
    store->SetFlushState(EStoreFlushState::Running);
}

void TStoreManager::EndStoreFlush(TDynamicMemoryStorePtr store)
{
    YCHECK(store->GetFlushState() == EStoreFlushState::Running);
    store->SetFlushState(EStoreFlushState::Complete);
}

void TStoreManager::BackoffStoreFlush(TDynamicMemoryStorePtr store)
{
    YCHECK(store->GetFlushState() == EStoreFlushState::Running);
    store->SetFlushState(EStoreFlushState::Failed);
    NConcurrency::TDelayedExecutor::Submit(
        BIND([=] () {
            if (store->GetFlushState() == EStoreFlushState::Failed) {
                store->SetFlushState(EStoreFlushState::None);
            }
        }).Via(Tablet_->GetEpochAutomatonInvoker()),
        Config_->ErrorBackoffTime);
}

bool TStoreManager::IsStoreCompactable(IStorePtr store)
{
    if (store->GetStoreState() != EStoreState::Persistent) {
        return false;
    }

    auto chunkStore = store->AsChunk();

    // NB: Partitioning chunk stores with backing ones may interfere with conflict checking.
    if (chunkStore->HasBackingStore()) {
        return false;
    }

    if (chunkStore->GetCompactionState() != EStoreCompactionState::None) {
        return false;
    }

    return true;
}

void TStoreManager::BeginStoreCompaction(TChunkStorePtr store)
{
    YCHECK(store->GetCompactionState() == EStoreCompactionState::None);
    store->SetCompactionState(EStoreCompactionState::Running);
}

void TStoreManager::EndStoreCompaction(TChunkStorePtr store)
{
    YCHECK(store->GetCompactionState() == EStoreCompactionState::Running);
    store->SetCompactionState(EStoreCompactionState::Complete);
}

void TStoreManager::BackoffStoreCompaction(TChunkStorePtr store)
{
    YCHECK(store->GetCompactionState() == EStoreCompactionState::Running);
    store->SetCompactionState(EStoreCompactionState::Failed);
    NConcurrency::TDelayedExecutor::Submit(
        BIND([=] () {
            if (store->GetCompactionState() == EStoreCompactionState::Failed) {
                store->SetCompactionState(EStoreCompactionState::None);
            }
        }).Via(Tablet_->GetEpochAutomatonInvoker()),
        Config_->ErrorBackoffTime);
}

void TStoreManager::ScheduleStorePreload(TChunkStorePtr store)
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

bool TStoreManager::TryPreloadStoreFromInterceptedData(
    TChunkStorePtr store,
    TInterceptedChunkDataPtr chunkData)
{
    auto state = store->GetPreloadState();
    YCHECK(state == EStorePreloadState::None);

    auto mode = Tablet_->GetConfig()->InMemoryMode;
    YCHECK(mode != EInMemoryMode::None);

    if (!chunkData) {
        LOG_WARNING("Intercepted chunk data for in-memory store is missing (StoreId: %v)",
            store->GetId());
        return false;
    }

    if (mode != chunkData->InMemoryMode) {
        LOG_WARNING("Intercepted chunk data for in-memory store has invalid mode (StoreId: %v, ExpectedMode: %v, ActualMode: %v)",
            store->GetId(),
            mode,
            chunkData->InMemoryMode);
        return false;
    }

    store->PreloadFromInterceptedData(chunkData);
    store->SetPreloadState(EStorePreloadState::Complete);

    LOG_INFO("In-memory store preloaded from intercepted chunk data (StoreId: %v, Mode: %v)",
        store->GetId(),
        mode);

    return true;
}

TChunkStorePtr TStoreManager::PeekStoreForPreload()
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

void TStoreManager::BeginStorePreload(TChunkStorePtr store, TFuture<void> future)
{
    YCHECK(store->GetId() == Tablet_->PreloadStoreIds().front());
    Tablet_->PreloadStoreIds().pop_front();
    store->SetPreloadState(EStorePreloadState::Running);
    store->SetPreloadFuture(future);
}

void TStoreManager::EndStorePreload(TChunkStorePtr store)
{
    store->SetPreloadState(EStorePreloadState::Complete);
    store->SetPreloadFuture(TFuture<void>());
}

void TStoreManager::BackoffStorePreload(TChunkStorePtr store)
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

void TStoreManager::BackoffStoreRemoval(IStorePtr store)
{
    NConcurrency::TDelayedExecutor::Submit(
        BIND([=] () {
            switch (store->GetType()) {
                case EStoreType::DynamicMemory: {
                    auto dynamicMemoryStore = store->AsDynamicMemory();
                    if (dynamicMemoryStore->GetFlushState() == EStoreFlushState::Complete) {
                        dynamicMemoryStore->SetFlushState(EStoreFlushState::None);
                    }
                    break;
                }
                case EStoreType::Chunk: {
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

void TStoreManager::Remount(TTableMountConfigPtr mountConfig, TTabletWriterOptionsPtr writerOptions)
{
    Tablet_->SetConfig(mountConfig);
    Tablet_->SetWriterOptions(writerOptions);

    UpdateInMemoryMode();
}

void TStoreManager::UpdateInMemoryMode()
{
    auto mode = Tablet_->GetConfig()->InMemoryMode;

    Tablet_->PreloadStoreIds().clear();

    for (const auto& pair : Tablet_->Stores()) {
        const auto& store = pair.second;
        if (store->GetType() == EStoreType::Chunk) {
            auto chunkStore = store->AsChunk();
            chunkStore->SetInMemoryMode(mode);
            if (mode != EInMemoryMode::None) {
                ScheduleStorePreload(chunkStore);
            }
        }
    }
}

bool TStoreManager::IsRecovery() const
{
    auto slot = Tablet_->GetSlot();
    // NB: Slot can be null in tests.
    return slot ? slot->GetHydraManager()->IsRecovery() : false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

