#include "stdafx.h"
#include "store_manager.h"
#include "tablet.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"
#include "transaction.h"
#include "config.h"
#include "tablet_slot.h"
#include "transaction_manager.h"
#include "private.h"

#include <core/misc/small_vector.h>

#include <core/concurrency/scheduler.h>

#include <ytlib/object_client/public.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/versioned_lookuper.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/transaction_client/transaction_manager.h>

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

static const auto BlockedRowWaitQuantum = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

TStoreManager::TStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* Tablet_)
    : Config_(config)
    , Tablet_(Tablet_)
    , RotationScheduled_(false)
    , LastRotated_(TInstant::Now())
    , Logger(TabletNodeLogger)
{
    YCHECK(Config_);
    YCHECK(Tablet_);
}

void TStoreManager::Initialize()
{
    Logger.AddTag("TabletId: %v", Tablet_->GetId());
    if (Tablet_->GetSlot()) {
        Logger.AddTag("CellId: %v", Tablet_->GetSlot()->GetCellId());
    }

    KeyColumnCount_ = Tablet_->GetKeyColumnCount();

    for (const auto& pair : Tablet_->Stores()) {
        auto store = pair.second;
        if (store->GetState() != EStoreState::ActiveDynamic) {
            MaxTimestampToStore_.insert(std::make_pair(store->GetMaxTimestamp(), store));
        }
    }
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
        auto state = store->GetState();
        if (state != EStoreState::Persistent) {
            return true;
        }
    }
    return false;
}

TDynamicRowRef TStoreManager::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prelock,
    ELockMode lockMode)
{
    ValidateServerDataRow(row, KeyColumnCount_, Tablet_->Schema());

    YASSERT(row.GetCount() >= KeyColumnCount_);
    if (row.GetCount() == KeyColumnCount_) {
        THROW_ERROR_EXCEPTION("Empty writes are not allowed")
            << TErrorAttribute("transaction_id", transaction->GetId())
            << TErrorAttribute("tablet_id", Tablet_->GetId())
            << TErrorAttribute("key", row);
    }

    ui32 lockMask = ComputeLockMask(row, lockMode);

    if (prelock) {
        CheckInactiveStoresLocks(
            transaction,
            row,
            lockMask);
    }

    TDynamicMemoryStorePtr store;
    TDynamicRow dynamicRow;
    do {
        store = Tablet_->GetActiveStore();
        dynamicRow = store->WriteRow(
            transaction,
            row,
            prelock,
            lockMask);
    } while (!dynamicRow);

    return TDynamicRowRef(store.Get(), dynamicRow);
}

TDynamicRowRef TStoreManager::DeleteRow(
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

    TDynamicMemoryStorePtr store;
    TDynamicRow dynamicRow;
    do {
        store = Tablet_->GetActiveStore();
        dynamicRow = store->DeleteRow(
            transaction,
            key,
            prelock);
    } while (!dynamicRow);

    return TDynamicRowRef(store.Get(), dynamicRow);
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

ui32 TStoreManager::ComputeLockMask(TUnversionedRow row, ELockMode lockMode)
{
    switch (lockMode) {
        case ELockMode::Row:
            return TDynamicRow::PrimaryLockMask;

        case ELockMode::Column: {
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

        default:
            YUNREACHABLE();
    }
}

void TStoreManager::CheckInactiveStoresLocks(
    TTransaction* transaction,
    TUnversionedRow key,
    ui32 lockMask)
{
    for (const auto& store : LockedStores_) {
        store->CheckRowLocks(
            key,
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
            key,
            transaction,
            lockMask);
    }
}

void TStoreManager::CheckForUnlockedStore(TDynamicMemoryStore * store)
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
        store->GetAlignedPoolCapacity() >= config->MaxMemoryStoreAlignedPoolSize ||
        store->GetUnalignedPoolCapacity() >= config->MaxMemoryStoreUnalignedPoolSize;
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
    if (store->GetAlignedPoolCapacity() <=  2 * Config_->AlignedPoolChunkSize &&
        store->GetUnalignedPoolCapacity() <= 2 * Config_->UnalignedPoolChunkSize)
    {
        return false;
    }

    return true;
}

bool TStoreManager::IsRotationScheduled() const
{
    return RotationScheduled_;
}

void TStoreManager::SetRotationScheduled()
{
    if (RotationScheduled_) 
        return;
    
    RotationScheduled_ = true;

    LOG_INFO("Tablet store rotation scheduled");
}

void TStoreManager::ResetRotationScheduled()
{
    if (!RotationScheduled_)
        return;

    RotationScheduled_ = false;

    LOG_INFO_UNLESS(IsRecovery(), "Tablet store rotation canceled");
}

void TStoreManager::RotateStores(bool createNew)
{
    RotationScheduled_ = false;
    LastRotated_ = TInstant::Now();

    auto store = Tablet_->GetActiveStore();
    YCHECK(store);

    store->SetState(EStoreState::PassiveDynamic);

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

    if (createNew) {
        CreateActiveStore();
    } else {
        Tablet_->SetActiveStore(nullptr);
    }

    LOG_INFO_UNLESS(IsRecovery(), "Tablet stores rotated");
}

void TStoreManager::AddStore(IStorePtr store)
{
    YCHECK(store->GetType() == EStoreType::Chunk);

    Tablet_->AddStore(store);
    MaxTimestampToStore_.insert(std::make_pair(store->GetMaxTimestamp(), store));
}

void TStoreManager::RemoveStore(IStorePtr store)
{
    YASSERT(store->GetState() != EStoreState::ActiveDynamic);

    store->SetState(EStoreState::Removed);
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
    auto* slot = Tablet_->GetSlot();
    // NB: For tests mostly.
    auto storeId = slot ? slot->GenerateId(EObjectType::DynamicMemoryTabletStore) : TStoreId::Create();
    auto store = CreateDynamicMemoryStore(storeId);
    Tablet_->AddStore(store);
    Tablet_->SetActiveStore(store);

    LOG_INFO_UNLESS(IsRecovery(), "Active store created (StoreId: %v)",
        storeId);
}

TDynamicMemoryStorePtr TStoreManager::CreateDynamicMemoryStore(const TStoreId& storeId)
{
    auto store = New<TDynamicMemoryStore>(
        Config_,
        storeId,
        Tablet_);
    
    auto slot = Tablet_->GetSlot();
    // NB: Slot can be null in tests.
    if (slot) {
        store->SubscribeRowBlocked(BIND(
            &TStoreManager::OnRowBlocked,
            MakeWeak(this),
            store.Get(),
            slot));
    }

    return store;
}

TChunkStorePtr TStoreManager::CreateChunkStore(
    NCellNode::TBootstrap* bootstrap,
    const TChunkId& chunkId,
    const TChunkMeta* chunkMeta)
{
    return New<TChunkStore>(
        chunkId,
        Tablet_,
        chunkMeta,
        bootstrap);
}

bool TStoreManager::IsStoreLocked(TDynamicMemoryStorePtr store) const
{
    return LockedStores_.find(store) != LockedStores_.end();
}

const yhash_set<TDynamicMemoryStorePtr>& TStoreManager::GetLockedStores() const
{
    return LockedStores_;
}

bool TStoreManager::IsRecovery() const
{
    auto slot = Tablet_->GetSlot();
    // NB: Slot can be null in tests.
    return slot ? slot->GetHydraManager()->IsRecovery() : false;
}

void TStoreManager::OnRowBlocked(
    IStore* store,
    TTabletSlotPtr slot,
    TDynamicRow row,
    int lockIndex)
{
    WaitFor(
        BIND(
            &TStoreManager::WaitForBlockedRow,
            MakeStrong(this),
            MakeStrong(store),
            row,
            lockIndex)
        .AsyncVia(slot->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Read))
        .Run());
}

void TStoreManager::WaitForBlockedRow(
    IStorePtr /*store*/,
    TDynamicRow row,
    int lockIndex)
{
    const auto& lock = row.BeginLocks(KeyColumnCount_)[lockIndex];
    const auto* transaction = lock.Transaction;
    if (!transaction)
        return;

    LOG_DEBUG("Waiting on blocked row (Key: %v, LockIndex: %v, TransactionId: %v)",
        RowToKey(Tablet_, row),
        lockIndex,
        transaction->GetId());
    WaitFor(transaction->GetFinished().WithTimeout(BlockedRowWaitQuantum));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

