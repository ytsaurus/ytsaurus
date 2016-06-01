#include "sorted_store_manager.h"
#include "sorted_chunk_store.h"
#include "config.h"
#include "sorted_dynamic_store.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "transaction_manager.h"

#include <yt/ytlib/chunk_client/block_cache.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/versioned_reader.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>

#include <yt/ytlib/transaction_client/helpers.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NObjectClient;

using NTableClient::TKey;

////////////////////////////////////////////////////////////////////////////////

TSortedStoreManager::TSortedStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    ITabletContext* tabletContext,
    NHydra::IHydraManagerPtr hydraManager,
    TInMemoryManagerPtr inMemoryManager)
    : TStoreManagerBase(
        config,
        tablet,
        tabletContext,
        hydraManager,
        inMemoryManager)
    , KeyColumnCount_(Tablet_->GetKeyColumnCount())
{
    for (const auto& pair : Tablet_->Stores()) {
        auto store = pair.second->AsSorted();
        if (store->GetStoreState() != EStoreState::ActiveDynamic) {
            MaxTimestampToStore_.insert(std::make_pair(store->GetMaxTimestamp(), store));
        }
    }

    if (Tablet_->GetActiveStore()) {
        ActiveStore_ = Tablet_->GetActiveStore()->AsSortedDynamic();
    }

    // This schedules preload of in-memory tablets.
    UpdateInMemoryMode();
}

bool TSortedStoreManager::HasActiveLocks() const
{
    if (ActiveStore_->GetLockCount() > 0) {
        return true;
    }

    if (!LockedStores_.empty()) {
        return true;
    }

    return false;
}

void TSortedStoreManager::ExecuteAtomicWrite(
    TTablet* tablet,
    TTransaction* transaction,
    TWireProtocolReader* reader,
    bool prelock)
{
    auto command = reader->ReadCommand();
    switch (command) {
        case EWireProtocolCommand::WriteRow: {
            TReqWriteRow req;
            reader->ReadMessage(&req);
            auto row = reader->ReadUnversionedRow();
            WriteRowAtomic(
                transaction,
                row,
                prelock);
            break;
        }

        case EWireProtocolCommand::DeleteRow: {
            TReqDeleteRow req;
            reader->ReadMessage(&req);
            auto key = reader->ReadUnversionedRow();
            DeleteRowAtomic(
                transaction,
                key,
                prelock);
            break;
        }

        default:
            THROW_ERROR_EXCEPTION("Unknown write command %v",
                command);
    }
}

void TSortedStoreManager::ExecuteNonAtomicWrite(
    TTablet* tablet,
    TTimestamp commitTimestamp,
    TWireProtocolReader* reader)
{
    auto command = reader->ReadCommand();
    switch (command) {
        case EWireProtocolCommand::WriteRow: {
            TReqWriteRow req;
            reader->ReadMessage(&req);
            auto row = reader->ReadUnversionedRow();
            WriteRowNonAtomic(commitTimestamp, row);
            break;
        }

        case EWireProtocolCommand::DeleteRow: {
            TReqDeleteRow req;
            reader->ReadMessage(&req);
            auto key = reader->ReadUnversionedRow();
            DeleteRowNonAtomic(commitTimestamp, key);
            break;
        }

        default:
            THROW_ERROR_EXCEPTION("Unknown write command %v",
                command);
    }
}

TSortedDynamicRowRef TSortedStoreManager::WriteRowAtomic(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prelock)
{
    if (prelock) {
        ValidateOnWrite(transaction->GetId(), row);
    }

    ui32 lockMask = ComputeLockMask(row);

    if (prelock) {
        CheckInactiveStoresLocks(
            transaction,
            row,
            lockMask);
    }

    auto dynamicRow = ActiveStore_->WriteRowAtomic(transaction, row, lockMask);
    auto dynamicRowRef = TSortedDynamicRowRef(ActiveStore_.Get(), this, dynamicRow);
    LockRow(transaction, prelock, dynamicRowRef);
    return dynamicRowRef;
}

void TSortedStoreManager::WriteRowNonAtomic(
    TTimestamp commitTimestamp,
    TUnversionedRow row)
{
    // TODO(sandello): YT-4148
    // ValidateOnWrite(transactionId, row);

    ActiveStore_->WriteRowNonAtomic(row, commitTimestamp);
}

TSortedDynamicRowRef TSortedStoreManager::DeleteRowAtomic(
    TTransaction* transaction,
    NTableClient::TKey key,
    bool prelock)
{
    if (prelock) {
        ValidateOnDelete(transaction->GetId(), key);

        CheckInactiveStoresLocks(
            transaction,
            key,
            TSortedDynamicRow::PrimaryLockMask);
    }

    auto dynamicRow = ActiveStore_->DeleteRowAtomic(transaction, key);
    auto dynamicRowRef = TSortedDynamicRowRef(ActiveStore_.Get(), this, dynamicRow);
    LockRow(transaction, prelock, dynamicRowRef);
    return dynamicRowRef;
}

void TSortedStoreManager::DeleteRowNonAtomic(
    TTimestamp commitTimestamp,
    NTableClient::TKey key)
{
    // TODO(sandello): YT-4148
    // ValidateOnDelete(transactionId, key);

    ActiveStore_->DeleteRowNonAtomic(key, commitTimestamp);
}

void TSortedStoreManager::LockRow(TTransaction* transaction, bool prelock, const TSortedDynamicRowRef& rowRef)
{
    if (prelock) {
        transaction->PrelockedRows().push(rowRef);
    } else {
        transaction->LockedRows().push_back(rowRef);
    }
}

void TSortedStoreManager::ConfirmRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    transaction->LockedRows().push_back(rowRef);
}

void TSortedStoreManager::PrepareRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    rowRef.Store->PrepareRow(transaction, rowRef.Row);
}

void TSortedStoreManager::CommitRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    if (rowRef.Store == ActiveStore_) {
        ActiveStore_->CommitRow(transaction, rowRef.Row);
    } else {
        auto migratedRow = ActiveStore_->MigrateRow(transaction, rowRef.Row);
        rowRef.Store->CommitRow(transaction, rowRef.Row);
        CheckForUnlockedStore(rowRef.Store);
        ActiveStore_->CommitRow(transaction, migratedRow);
    }
}

void TSortedStoreManager::AbortRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    rowRef.Store->AbortRow(transaction, rowRef.Row);
    CheckForUnlockedStore(rowRef.Store);
}

ui32 TSortedStoreManager::ComputeLockMask(TUnversionedRow row)
{
    const auto& columnIndexToLockIndex = Tablet_->ColumnIndexToLockIndex();
    ui32 lockMask = 0;
    for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
        const auto& value = row[index];
        int lockIndex = columnIndexToLockIndex[value.Id];
        lockMask |= (1 << lockIndex);
    }
    Y_ASSERT(lockMask != 0);
    return lockMask;
}

void TSortedStoreManager::CheckInactiveStoresLocks(
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
        if (store->GetType() == EStoreType::SortedDynamic &&
            store->AsSortedDynamic()->GetLockCount() > 0)
            continue;
        store->CheckRowLocks(
            row,
            transaction,
            lockMask);
    }
}

void TSortedStoreManager::CheckForUnlockedStore(TSortedDynamicStore* store)
{
    if (store == ActiveStore_ || store->GetLockCount() > 0)
        return;

    LOG_INFO_UNLESS(IsRecovery(), "Store unlocked and will be dropped (StoreId: %v)",
        store->GetId());
    YCHECK(LockedStores_.erase(store) == 1);
}

bool TSortedStoreManager::IsOverflowRotationNeeded() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    const auto& config = Tablet_->GetConfig();
    return
        ActiveStore_->GetKeyCount() >= config->MaxDynamicStoreKeyCount ||
        ActiveStore_->GetValueCount() >= config->MaxDynamicStoreValueCount ||
        ActiveStore_->GetPoolCapacity() >= config->MaxDynamicStorePoolSize;
}

bool TSortedStoreManager::IsPeriodicRotationNeeded() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    const auto& config = Tablet_->GetConfig();
    return
        TInstant::Now() > LastRotated_ + config->DynamicStoreAutoFlushPeriod &&
        ActiveStore_->GetKeyCount() > 0;
}

bool TSortedStoreManager::IsRotationPossible() const
{
    if (IsRotationScheduled()) {
        return false;
    }

    if (!ActiveStore_) {
        return false;
    }

    return true;
}

bool TSortedStoreManager::IsForcedRotationPossible() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    // Check for "almost" initial size.
    if (ActiveStore_->GetPoolCapacity() <= 2 * Config_->PoolChunkSize) {
        return false;
    }

    return true;
}

void TSortedStoreManager::Rotate(bool createNewStore)
{
    RotationScheduled_ = false;
    LastRotated_ = TInstant::Now();

    YCHECK(ActiveStore_);
    ActiveStore_->SetStoreState(EStoreState::PassiveDynamic);

    if (ActiveStore_->GetLockCount() > 0) {
        LOG_INFO_UNLESS(IsRecovery(), "Active store is locked and will be kept (StoreId: %v, LockCount: %v)",
            ActiveStore_->GetId(),
            ActiveStore_->GetLockCount());
        YCHECK(LockedStores_.insert(ActiveStore_).second);
    } else {
        LOG_INFO_UNLESS(IsRecovery(), "Active store is not locked and will be dropped (StoreId: %v)",
            ActiveStore_->GetId(),
            ActiveStore_->GetLockCount());
    }

    MaxTimestampToStore_.insert(std::make_pair(ActiveStore_->GetMaxTimestamp(), ActiveStore_));

    if (createNewStore) {
        CreateActiveStore();
    } else {
        ActiveStore_.Reset();
        Tablet_->SetActiveStore(nullptr);
    }

    LOG_INFO_UNLESS(IsRecovery(), "Tablet stores rotated");
}

void TSortedStoreManager::AddStore(IStorePtr store, bool onMount)
{
    TStoreManagerBase::AddStore(store, onMount);

    auto sortedStore = store->AsSorted();
    MaxTimestampToStore_.insert(std::make_pair(sortedStore->GetMaxTimestamp(), sortedStore));
}

void TSortedStoreManager::RemoveStore(IStorePtr store)
{
    TStoreManagerBase::RemoveStore(store);

    // The range is likely to contain at most one element.
    auto sortedStore = store->AsSorted();
    auto range = MaxTimestampToStore_.equal_range(sortedStore->GetMaxTimestamp());
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second == sortedStore) {
            MaxTimestampToStore_.erase(it);
            break;
        }
    }
}

void TSortedStoreManager::CreateActiveStore()
{
    auto storeId = TabletContext_->GenerateId(EObjectType::SortedDynamicTabletStore);
    ActiveStore_ = TabletContext_
        ->CreateStore(Tablet_, EStoreType::SortedDynamic, storeId)
        ->AsSortedDynamic();

    Tablet_->AddStore(ActiveStore_);
    Tablet_->SetActiveStore(ActiveStore_);

    LOG_INFO_UNLESS(IsRecovery(), "Active store created (StoreId: %v)",
        storeId);
}

bool TSortedStoreManager::IsStoreLocked(IStorePtr store) const
{
    return LockedStores_.find(store->AsSortedDynamic()) != LockedStores_.end();
}

std::vector<IStorePtr> TSortedStoreManager::GetLockedStores() const
{
    return std::vector<IStorePtr>(LockedStores_.begin(), LockedStores_.end());
}

bool TSortedStoreManager::IsStoreCompactable(IStorePtr store) const
{
    if (store->GetStoreState() != EStoreState::Persistent) {
        return false;
    }

    // NB: Partitioning chunk stores with backing ones may interfere with conflict checking.
    auto sortedChunkStore = store->AsSortedChunk();
    if (sortedChunkStore->HasBackingStore()) {
        return false;
    }

    if (sortedChunkStore->GetCompactionState() != EStoreCompactionState::None) {
        return false;
    }

    return true;
}

void TSortedStoreManager::ValidateActiveStoreOverflow()
{
    const auto& config = Tablet_->GetConfig();

    {
        auto keyCount = ActiveStore_->GetKeyCount();
        auto keyLimit = config->MaxDynamicStoreKeyCount;
        if (keyCount >= keyLimit) {
            THROW_ERROR_EXCEPTION("Active store is over key capacity")
                << TErrorAttribute("tablet_id", Tablet_->GetId())
                << TErrorAttribute("key_count", keyCount)
                << TErrorAttribute("key_limit", keyLimit);
        }
    }

    {
        auto valueCount = ActiveStore_->GetValueCount();
        auto valueLimit = config->MaxDynamicStoreValueCount;
        if (valueCount >= valueLimit) {
            THROW_ERROR_EXCEPTION("Active store is over value capacity")
                << TErrorAttribute("tablet_id", Tablet_->GetId())
                << TErrorAttribute("value_count", valueCount)
                << TErrorAttribute("value_limit", valueLimit);
        }
    }

    {
        auto poolCapacity = ActiveStore_->GetPoolCapacity();
        auto poolCapacityLimit = config->MaxDynamicStorePoolSize;
        if (poolCapacity >= poolCapacityLimit) {
            THROW_ERROR_EXCEPTION("Active store is over its memory pool capacity")
                << TErrorAttribute("tablet_id", Tablet_->GetId())
                << TErrorAttribute("pool_capacity", poolCapacity)
                << TErrorAttribute("pool_capacity_limit", poolCapacityLimit);
        }
    }
}

void TSortedStoreManager::ValidateOnWrite(
    const TTransactionId& transactionId,
    TUnversionedRow row)
{
    try {
        ValidateServerDataRow(row, KeyColumnCount_, Tablet_->Schema());
        if (row.GetCount() == KeyColumnCount_) {
            THROW_ERROR_EXCEPTION("Empty writes are not allowed");
        }
    } catch (TErrorException& ex) {
        auto& errorAttributes = ex.Error().Attributes();
        errorAttributes.SetYson("transaction_id", NYTree::ConvertToYsonString(transactionId));
        errorAttributes.SetYson("tablet_id", NYTree::ConvertToYsonString(Tablet_->GetId()));
        errorAttributes.SetYson("row", NYTree::ConvertToYsonString(row));
        throw ex;
    }
}

void TSortedStoreManager::ValidateOnDelete(
    const TTransactionId& transactionId,
    TKey key)
{
    try {
        ValidateServerKey(key, KeyColumnCount_, Tablet_->Schema());
    } catch (TErrorException& ex) {
        auto& errorAttributes = ex.Error().Attributes();
        errorAttributes.SetYson("transaction_id", NYTree::ConvertToYsonString(transactionId));
        errorAttributes.SetYson("tablet_id", NYTree::ConvertToYsonString(Tablet_->GetId()));
        errorAttributes.SetYson("key", NYTree::ConvertToYsonString(key));
        throw ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

