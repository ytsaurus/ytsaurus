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
            THROW_ERROR_EXCEPTION("Unsupported write command %v",
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
        transaction->PrelockedSortedRows().push(rowRef);
    } else {
        transaction->LockedSortedRows().push_back(rowRef);
    }
}

void TSortedStoreManager::ConfirmRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    transaction->LockedSortedRows().push_back(rowRef);
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

IDynamicStore* TSortedStoreManager::GetActiveStore() const
{
    return ActiveStore_.Get();
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
    YASSERT(lockMask != 0);
    return lockMask;
}

void TSortedStoreManager::CheckInactiveStoresLocks(
    TTransaction* transaction,
    TUnversionedRow row,
    ui32 lockMask)
{
    for (const auto& store : LockedStores_) {
        store->AsSortedDynamic()->CheckRowLocks(
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

void TSortedStoreManager::ResetActiveStore()
{
    ActiveStore_.Reset();
}

void TSortedStoreManager::OnActiveStoreRotated()
{
    MaxTimestampToStore_.insert(std::make_pair(ActiveStore_->GetMaxTimestamp(), ActiveStore_));
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
        errorAttributes.Set("transaction_id", transactionId);
        errorAttributes.Set("tablet_id", Tablet_->GetId());
        errorAttributes.Set("row", row);
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
        errorAttributes.Set("transaction_id", transactionId);
        errorAttributes.Set("tablet_id", Tablet_->GetId());
        errorAttributes.Set("key", key);
        throw ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

