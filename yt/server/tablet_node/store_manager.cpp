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

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/versioned_lookuper.h>

#include <ytlib/tablet_client/config.h>

#include <server/hydra/hydra_manager.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NTabletClient;
using namespace NObjectClient;

using NVersionedTableClient::TKey;

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerRead = 1024;

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

        ColumnFilter_ = reader->ReadColumnFilter();

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

    Logger.AddTag("TabletId: %v", Tablet_->GetId());
    if (Tablet_->GetSlot()) {
        Logger.AddTag("CellId: %v", Tablet_->GetSlot()->GetCellGuid());
    }
}

void TStoreManager::Initialize()
{
    for (const auto& pair : Tablet_->Stores()) {
        auto store = pair.second;
        if (store->GetType() == EStoreType::DynamicMemory) {
            if (store != Tablet_->GetActiveStore()) {
                YCHECK(PassiveStores_.insert(store->AsDynamicMemory()).second);
            }
        } else {
            LatestTimestampToStore_.insert(std::make_pair(store->GetMaxTimestamp(), store));
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
    bool prelock)
{
    ValidateServerDataRow(row, Tablet_->GetKeyColumnCount(), Tablet_->Schema());

    auto* store = FindRelevantStoreAndCheckLocks(
        transaction,
        row,
        ERowLockMode::Write,
        prelock);

    auto updatedRow = store->WriteRow(
        transaction,
        row,
        prelock);

    return updatedRow ? TDynamicRowRef(store, updatedRow) : TDynamicRowRef();
}

TDynamicRowRef TStoreManager::DeleteRow(
    TTransaction* transaction,
    NVersionedTableClient::TKey key,
    bool prelock)
{
    ValidateServerKey(key, Tablet_->GetKeyColumnCount(), Tablet_->Schema());

    auto* store = FindRelevantStoreAndCheckLocks(
        transaction,
        key,
        ERowLockMode::Delete,
        prelock);

    auto updatedRow = store->DeleteRow(
        transaction,
        key,
        prelock);

    return updatedRow ? TDynamicRowRef(store, updatedRow) : TDynamicRowRef();
}

void TStoreManager::ConfirmRow(const TDynamicRowRef& rowRef)
{
    rowRef.Store->ConfirmRow(rowRef.Row);
}

void TStoreManager::PrepareRow(const TDynamicRowRef& rowRef)
{
    rowRef.Store->PrepareRow(rowRef.Row);
}

void TStoreManager::CommitRow(const TDynamicRowRef& rowRef)
{
    auto row = MigrateRowIfNeeded(rowRef);
    Tablet_->GetActiveStore()->CommitRow(row);
}

void TStoreManager::AbortRow(const TDynamicRowRef& rowRef)
{
    rowRef.Store->AbortRow(rowRef.Row);
    CheckForUnlockedStore(rowRef.Store);
}

TDynamicRow TStoreManager::MigrateRowIfNeeded(const TDynamicRowRef& rowRef)
{
    if (rowRef.Store->GetState() == EStoreState::ActiveDynamic) {
        return rowRef.Row;
    }

    // NB: MigrateRow may change rowRef if the latter references
    // an element from transaction->LockedRows().
    auto* store = rowRef.Store;
    auto migratedRow = Tablet_->GetActiveStore()->MigrateRow(rowRef);
    CheckForUnlockedStore(store);
    return migratedRow;
}

TDynamicMemoryStore* TStoreManager::FindRelevantStoreAndCheckLocks(
    TTransaction* transaction,
    TUnversionedRow key,
    ERowLockMode mode,
    bool prelock)
{
    // Check locked stored.
    for (const auto& store : LockedStores_) {
        auto row = store->FindRowAndCheckLocks(
            key,
            transaction,
            mode);
        if (row) {
            return store.Get();
        }
    } 

    if (prelock) {
        // Check passive stores.
        for (const auto& store : PassiveStores_) {
            // Skip locked stores (already checked, see above).
            if (store->GetLockCount() > 0)
                continue;
            
            auto row = store->FindRowAndCheckLocks(
                key,
                transaction,
                mode);

            // Only active or locked stores can be relevant.
            YCHECK(!row);
        }

        // Check chunk stores.
        bool logged = false;
        auto startTimestamp = transaction->GetStartTimestamp();
        for (auto it = LatestTimestampToStore_.rbegin();
             it != LatestTimestampToStore_.rend() && it->first > startTimestamp;
             ++it)
        {
            // NB: Hold by value, LatestTimestampToStore_ may be changed in a concurrent fiber. 
            auto store = it->second;

            if (!logged && store->GetType() == EStoreType::Chunk) {
                LOG_WARNING("Checking chunk stores for conflicting commits (TransactionId: %v, StartTimestamp: %v)",
                    transaction->GetId(),
                    startTimestamp);
                logged = true;
            }

            auto latestTimestamp = store->GetLatestCommitTimestamp(key);
            if (latestTimestamp > startTimestamp) {
                THROW_ERROR_EXCEPTION("Row lock conflict")
                    << TErrorAttribute("conflicted_transaction_id", transaction->GetId())
                    << TErrorAttribute("winner_transaction_commit_timestamp", latestTimestamp)
                    << TErrorAttribute("tablet_id", Tablet_->GetId())
                    << TErrorAttribute("key", key);
            }
        }
    }

    return Tablet_->GetActiveStore().Get();
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
    return
        TInstant::Now() > LastRotated_ + Config_->AutoFlushPeriod &&
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

    auto activeStore = Tablet_->GetActiveStore();
    YCHECK(activeStore);
    activeStore->SetState(EStoreState::PassiveDynamic);

    if (activeStore->GetLockCount() > 0) {
        LOG_INFO_UNLESS(IsRecovery(), "Active store is locked and will be kept (StoreId: %v, LockCount: %v)",
            activeStore->GetId(),
            activeStore->GetLockCount());
        YCHECK(LockedStores_.insert(activeStore).second);
    }

    YCHECK(PassiveStores_.insert(activeStore).second);
    LOG_INFO_UNLESS(IsRecovery(), "Passive store registered (StoreId: %v)",
        activeStore->GetId());

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
    LatestTimestampToStore_.insert(std::make_pair(store->GetMaxTimestamp(), store));
}

void TStoreManager::RemoveStore(IStorePtr store)
{
    store->SetState(EStoreState::Removed);
    Tablet_->RemoveStore(store);

    if (store->GetType() == EStoreType::DynamicMemory) {
        YCHECK(PassiveStores_.erase(store->AsDynamicMemory()) == 1);
        LOG_INFO_UNLESS(IsRecovery(), "Passive store unregistered (StoreId: %v)",
            store->GetId());
    } else {
        // The range is likely to have one element.
        auto range = LatestTimestampToStore_.equal_range(store->GetMaxTimestamp());
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == store) {
                LatestTimestampToStore_.erase(it);
                break;
            }
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
}

TDynamicMemoryStorePtr TStoreManager::CreateDynamicMemoryStore(const TStoreId& storeId)
{
    auto store = New<TDynamicMemoryStore>(
        Config_,
        storeId,
        Tablet_);
    
    // NB: Slot can be null in tests.
    if (Tablet_->GetSlot()) {
        store->SubscribeRowBlocked(BIND(&TStoreManager::OnRowBlocked, MakeWeak(this)));
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

void TStoreManager::OnRowBlocked(TDynamicRow row)
{
    WaitFor(BIND(&TStoreManager::WaitForBlockedRow, row)
        .AsyncVia(Tablet_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Read))
        .Run());
}

void TStoreManager::WaitForBlockedRow(TDynamicRow row)
{
    auto* transaction = row.GetTransaction();
    if (transaction) {
        WaitFor(transaction->GetFinished());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

