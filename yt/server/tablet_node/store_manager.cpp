#include "stdafx.h"
#include "store_manager.h"
#include "tablet.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"
#include "transaction.h"
#include "config.h"
#include "tablet_slot.h"
#include "row_merger.h"
#include "private.h"

#include <core/misc/small_vector.h>
#include <core/misc/object_pool.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/parallel_collector.h>

#include <ytlib/object_client/public.h>

#include <ytlib/tablet_client/wire_protocol.h>
#include <ytlib/tablet_client/wire_protocol.pb.h>

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

struct TLookupPoolTag { };

class TLookupExecutor
{
public:
    TLookupExecutor()
        : MemoryPool_(TLookupPoolTag())
        , RunCallback_(BIND(&TLookupExecutor::DoRun, this).Guarded())
    { }

    void Prepare(
        TTablet* tablet,
        TTimestamp timestamp,
        TWireProtocolReader* reader)
    {
        Tablet_ = tablet;
        KeyColumnCount_ = Tablet_->GetKeyColumnCount();
        SchemaColumnCount_ = Tablet_->GetSchemaColumnCount();

        TReqLookupRows req;
        reader->ReadMessage(&req);

        if (req.has_column_filter()) {
            ColumnFilter_.All = false;
            ColumnFilter_.Indexes = FromProto<int, SmallVector<int, TypicalColumnCount>>(req.column_filter().indexes());
        }

        SmallVector<bool, TypicalColumnCount> columnFilterFlags(SchemaColumnCount_);
        if (ColumnFilter_.All) {
            for (int id = 0; id < SchemaColumnCount_; ++id) {
                columnFilterFlags[id] = true;
            }
        } else {
            for (int index : ColumnFilter_.Indexes) {
                if (index < 0 || index >= SchemaColumnCount_) {
                    THROW_ERROR_EXCEPTION("Invalid index %v in column filter",
                        index);
                }
                columnFilterFlags[index] = true;
            }
        }

        // NB: IStore::CreateLookuper may yield so one must copy all stores.
        for (const auto& pair : Tablet_->Stores()) {
            Stores_.push_back(pair.second);
        }

        for (const auto& store : Stores_) {
            auto lookuper = store->CreateLookuper(timestamp, ColumnFilter_);
            Lookupers_.push_back(lookuper);
        }

        reader->ReadUnversionedRowset(&LookupKeys_);
    }

    TAsyncError Run(
        IInvokerPtr invoker,
        TWireProtocolWriter* writer)
    {
        if (invoker) {
            return RunCallback_.AsyncVia(invoker).Run(writer);
        } else {
            return MakeFuture(RunCallback_.Run(writer));
        }
    }

    const std::vector<TUnversionedRow>& GetLookupKeys() const
    {
        return LookupKeys_;
    }


    void Clean()
    {
        ColumnFilter_ = TColumnFilter();
        MemoryPool_.Clear();
        LookupKeys_.clear();
        Stores_.clear();
        Lookupers_.clear();
    }

private:
    TChunkedMemoryPool MemoryPool_;
    std::vector<TUnversionedRow> LookupKeys_;
    std::vector<IStorePtr> Stores_;
    std::vector<IVersionedLookuperPtr> Lookupers_;
    
    TTablet* Tablet_;
    int KeyColumnCount_;
    int SchemaColumnCount_;
    TColumnFilter ColumnFilter_;

    TCallback<TError(TWireProtocolWriter* writer)> RunCallback_;


    void DoRun(TWireProtocolWriter* writer)
    {
        TUnversionedRowMerger rowMerger(
            &MemoryPool_,
            SchemaColumnCount_,
            KeyColumnCount_,
            ColumnFilter_);

        TKeyComparer keyComparer(KeyColumnCount_);

        for (auto key : LookupKeys_) {
            ValidateServerKey(key, KeyColumnCount_, Tablet_->Schema());

            // Create lookupers, send requests, collect sync responses.
            TIntrusivePtr<TParallelCollector<TVersionedRow>> collector;
            SmallVector<TVersionedRow, TypicalStoreCount> partialRows;
            for (const auto& lookuper : Lookupers_) {
                auto futureRowOrError = lookuper->Lookup(key);
                auto maybeRowOrError = futureRowOrError.TryGet();
                if (maybeRowOrError) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(*maybeRowOrError);
                    partialRows.push_back(maybeRowOrError->Value());
                } else {
                    if (!collector) {
                        collector = New<TParallelCollector<TVersionedRow>>();
                    }
                    collector->Collect(futureRowOrError);
                }
            }

            // Wait for async responses.
            if (collector) {
                auto result = WaitFor(collector->Complete());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                partialRows.insert(partialRows.end(), result.Value().begin(), result.Value().end());
            }

            // Merge partial rows.
            for (auto row : partialRows) {
                if (row) {
                    rowMerger.AddPartialRow(row);
                }
            }

            auto mergedRow = rowMerger.BuildMergedRowAndReset();
            writer->WriteUnversionedRow(mergedRow);
        }
    }

};

} // namespace NTabletNode
} // namespace NYT

namespace NYT {

template <>
struct TPooledObjectTraits<NTabletNode::TLookupExecutor, void>
    : public TPooledObjectTraitsBase
{
    static void Clean(NTabletNode::TLookupExecutor* executor)
    {
        executor->Clean();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TStoreManager::TStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* Tablet_,
    IInvokerPtr readPoolInvoker)
    : Config_(config)
    , Tablet_(Tablet_)
    , ReadWorkerInvoker_(readPoolInvoker)
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
        Logger.AddTag("CellId: %v", Tablet_->GetSlot()->GetCellGuid());
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

void TStoreManager::LookupRows(
    TTimestamp timestamp,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    auto executor = ObjectPool<TLookupExecutor>().Allocate();
    executor->Prepare(Tablet_, timestamp, reader);
    LOG_DEBUG("Looking up %v keys", executor->GetLookupKeys().size());

    auto result = WaitFor(executor->Run(ReadWorkerInvoker_, writer));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
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

    auto* store = Tablet_->GetActiveStore().Get();
    auto dynamicRow = store->WriteRow(
        transaction,
        row,
        prelock,
        lockMask);

    return TDynamicRowRef(store, dynamicRow);
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

    auto* store = Tablet_->GetActiveStore().Get();
    auto dynamicRow = store->DeleteRow(
        transaction,
        key,
        prelock);

    return TDynamicRowRef(store, dynamicRow);
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
    auto row = MigrateRowIfNeeded(transaction, rowRef);
    Tablet_->GetActiveStore()->CommitRow(transaction, row);
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

TDynamicRow TStoreManager::MigrateRowIfNeeded(TTransaction* transaction, const TDynamicRowRef& rowRef)
{
    if (rowRef.Store->GetState() == EStoreState::ActiveDynamic) {
        return rowRef.Row;
    }

    // NB: MigrateRow may change rowRef if the latter references
    // an element from transaction->LockedRows().
    auto* store = rowRef.Store;
    auto migratedRow = Tablet_->GetActiveStore()->MigrateRow(transaction, rowRef);
    CheckForUnlockedStore(store);
    return migratedRow;
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
    
    // NB: Slot can be null in tests.
    if (Tablet_->GetSlot()) {
        store->SubscribeRowBlocked(BIND(&TStoreManager::OnRowBlocked, MakeWeak(this), store.Get()));
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

void TStoreManager::OnRowBlocked(IStore* store, TDynamicRow row, int lockIndex)
{
    const auto& lock = row.BeginLocks(KeyColumnCount_)[lockIndex];
    const auto* transaction = lock.Transaction;
    WaitFor(
        BIND(
            &TStoreManager::WaitForBlockedRow,
            MakeStrong(this),
            MakeStrong(store),
            row,
            lockIndex,
            transaction->GetId())
        .AsyncVia(Tablet_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Read))
        .Run());
}

void TStoreManager::WaitForBlockedRow(
    IStorePtr /*store*/,
    TDynamicRow row,
    int lockIndex,
    const TTransactionId& transactionId)
{
    const auto& lock = row.BeginLocks(KeyColumnCount_)[lockIndex];
    const auto* transaction = lock.Transaction;
    if (transaction && transaction->GetId() == transactionId) {
        WaitFor(transaction->GetFinished());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

