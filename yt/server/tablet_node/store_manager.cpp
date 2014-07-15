#include "stdafx.h"
#include "store_manager.h"
#include "tablet.h"
#include "dynamic_memory_store.h"
#include "transaction.h"
#include "config.h"
#include "tablet_slot.h"
#include "row_merger.h"
#include "private.h"

#include <core/misc/small_vector.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/parallel_collector.h>

#include <ytlib/object_client/public.h>

#include <ytlib/tablet_client/wire_protocol.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/schemaful_reader.h>

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

struct TLookupPoolTag { };

////////////////////////////////////////////////////////////////////////////////

TStoreManager::TStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* Tablet_)
    : Config_(config)
    , Tablet_(Tablet_)
    , RotationScheduled_(false)
    , LastRotated_(TInstant::Now())
    , LookupMemoryPool_(TLookupPoolTag())
    , Logger(TabletNodeLogger)
{
    YCHECK(Config_);
    YCHECK(Tablet_);

    VersionedPooledRows_.reserve(MaxRowsPerRead);

    if (Tablet_->GetSlot()) {
        Logger.AddTag("CellId: %v", Tablet_->GetSlot()->GetCellGuid());
    }

    for (const auto& pair : Tablet_->Stores()) {
        const auto& store = pair.second;
        if (store->GetType() == EStoreType::DynamicMemory && store != Tablet_->GetActiveStore()) {
            YCHECK(PassiveStores_.insert(store->AsDynamicMemory()).second);
        }
    }
}

TStoreManager::~TStoreManager()
{ }

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
    NTabletClient::TWireProtocolReader* reader,
    NTabletClient::TWireProtocolWriter* writer)
{
    auto columnFilter = reader->ReadColumnFilter();

    int keyColumnCount = Tablet_->GetKeyColumnCount();
    int schemaColumnCount = Tablet_->GetSchemaColumnCount();

    SmallVector<bool, TypicalColumnCount> columnFilterFlags(schemaColumnCount);
    if (columnFilter.All) {
        for (int id = 0; id < schemaColumnCount; ++id) {
            columnFilterFlags[id] = true;
        }
    } else {
        for (int index : columnFilter.Indexes) {
            if (index < 0 || index >= schemaColumnCount) {
                THROW_ERROR_EXCEPTION("Invalid index %d in column filter",
                    index);
            }
            columnFilterFlags[index] = true;
        }
    }

    PooledKeys_.clear();
    reader->ReadUnversionedRowset(&PooledKeys_);

    TUnversionedRowMerger rowMerger(
        &LookupMemoryPool_,
        schemaColumnCount,
        keyColumnCount,
        columnFilter);

    TKeyComparer keyComparer(keyColumnCount);

    UnversionedPooledRows_.clear();
    LookupMemoryPool_.Clear();

    for (auto key : PooledKeys_) {
        ValidateServerKey(key, keyColumnCount);

        auto lowerBound = TOwningKey(key);
        auto upperBound = GetKeySuccessor(key);

        // Construct readers.
        SmallVector<IVersionedReaderPtr, TypicalStoreCount> rowReaders;
        for (const auto& pair : Tablet_->Stores()) {
            const auto& store = pair.second;
            auto rowReader = store->CreateReader(
                lowerBound,
                upperBound,
                timestamp,
                columnFilter);
            if (rowReader) {
                rowReaders.push_back(std::move(rowReader));
            }
        }

        // Open readers.
        TIntrusivePtr<TParallelCollector<void>> openCollector;
        for (const auto& reader : rowReaders) {
            auto asyncResult = reader->Open();
            if (asyncResult.IsSet()) {
                THROW_ERROR_EXCEPTION_IF_FAILED(asyncResult.Get());
            } else {
                if (!openCollector) {
                    openCollector = New<TParallelCollector<void>>();
                }
                openCollector->Collect(asyncResult);
            }
        }

        if (openCollector) {
            auto result = WaitFor(openCollector->Complete());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        rowMerger.Start(lowerBound.Begin());

        // Merge values.
        for (const auto& reader : rowReaders) {
            VersionedPooledRows_.clear();
            // NB: Reading at most one row.
            reader->Read(&VersionedPooledRows_);
            if (VersionedPooledRows_.empty())
                continue;

            auto partialRow = VersionedPooledRows_[0];
            if (keyComparer(lowerBound, partialRow.BeginKeys()) != 0)
                continue;

            rowMerger.AddPartialRow(partialRow);
        }

        auto mergedRow = rowMerger.BuildMergedRow();
        UnversionedPooledRows_.push_back(mergedRow);
    }
    
    writer->WriteUnversionedRowset(UnversionedPooledRows_);
}

TDynamicRowRef TStoreManager::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prelock)
{
    ValidateServerDataRow(row, Tablet_->GetKeyColumnCount());

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
    ValidateServerKey(key, Tablet_->GetKeyColumnCount());

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
                LOG_WARNING("Checking chunk stores for conflicting commits (TransactionId: %s, StartTimestamp: %" PRIu64 ")",
                    ~ToString(transaction->GetId()),
                    startTimestamp);
                logged = true;
            }

            auto latestTimestamp = store->GetLatestCommitTimestamp(key);
            if (latestTimestamp > startTimestamp) {
                THROW_ERROR_EXCEPTION("Row lock conflict with a transaction committed at %" PRIu64,
                    latestTimestamp);
            }
        }
    }

    return Tablet_->GetActiveStore().Get();
}

void TStoreManager::CheckForUnlockedStore(TDynamicMemoryStore * store)
{
    if (store == Tablet_->GetActiveStore() || store->GetLockCount() > 0)
        return;

    LOG_INFO_UNLESS(IsRecovery(), "Store unlocked and will be dropped (TabletId: %s, StoreId: %s)",
        ~ToString(Tablet_->GetId()),
        ~ToString(store->GetId()));
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

    LOG_INFO("Tablet store rotation scheduled (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
}

void TStoreManager::ResetRotationScheduled()
{
    if (!RotationScheduled_)
        return;

    RotationScheduled_ = false;

    LOG_INFO_UNLESS(IsRecovery(), "Tablet store rotation canceled (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
}

void TStoreManager::RotateStores(bool createNew)
{
    RotationScheduled_ = false;
    LastRotated_ = TInstant::Now();

    auto activeStore = Tablet_->GetActiveStore();
    YCHECK(activeStore);
    activeStore->SetState(EStoreState::PassiveDynamic);

    if (activeStore->GetLockCount() > 0) {
        LOG_INFO_UNLESS(IsRecovery(), "Active store is locked and will be kept (TabletId: %s, StoreId: %s, LockCount: %d)",
            ~ToString(Tablet_->GetId()),
            ~ToString(activeStore->GetId()),
            activeStore->GetLockCount());
        YCHECK(LockedStores_.insert(activeStore).second);
    }

    YCHECK(PassiveStores_.insert(activeStore).second);
    LOG_INFO_UNLESS(IsRecovery(), "Passive store registered (TabletId: %s, StoreId: %s)",
        ~ToString(Tablet_->GetId()),
        ~ToString(activeStore->GetId()));

    if (createNew) {
        CreateActiveStore();
    } else {
        Tablet_->SetActiveStore(nullptr);
    }

    LOG_INFO_UNLESS(IsRecovery(), "Tablet stores rotated (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
}

void TStoreManager::AddStore(IStorePtr store)
{
    YCHECK(store->GetType() == EStoreType::Chunk);

    Tablet_->AddStore(store);

    auto latestTimestamp = store->GetMaxTimestamp();
    // Dynamic store returns MaxTimestamp.
    if (latestTimestamp != MaxTimestamp) {
        LatestTimestampToStore_.insert(std::make_pair(latestTimestamp, store));
    }
}

void TStoreManager::RemoveStore(IStorePtr store)
{
    store->SetState(EStoreState::Removed);
    Tablet_->RemoveStore(store);

    if (store->GetType() == EStoreType::DynamicMemory) {
        YCHECK(PassiveStores_.erase(store->AsDynamicMemory()) == 1);
        LOG_INFO_UNLESS(IsRecovery(), "Passive store unregistered (TabletId: %s, StoreId: %s)",
            ~ToString(Tablet_->GetId()),
            ~ToString(store->GetId()));
    }

    auto latestTimestamp = store->GetMaxTimestamp();
    // Dynamic store returns MaxTimestamp.
    if (latestTimestamp != MaxTimestamp) {
        auto range = LatestTimestampToStore_.equal_range(latestTimestamp);
        // The range is likely to have one element.
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
    auto id = slot ? slot->GenerateId(EObjectType::DynamicMemoryTabletStore) : TStoreId::Create();
 
    auto store = New<TDynamicMemoryStore>(
        Config_,
        id,
        Tablet_);

    Tablet_->AddStore(store);
    Tablet_->SetActiveStore(store);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
