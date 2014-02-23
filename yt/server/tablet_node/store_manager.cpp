#include "stdafx.h"
#include "store_manager.h"
#include "tablet.h"
#include "dynamic_memory_store.h"
#include "config.h"
#include "tablet_slot.h"
#include "row_merger.h"
#include "private.h"

#include <core/misc/small_vector.h>

#include <core/concurrency/fiber.h>
#include <core/concurrency/parallel_collector.h>

#include <ytlib/object_client/public.h>

#include <ytlib/tablet_client/wire_protocol.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/schemed_reader.h>

#include <ytlib/tablet_client/config.h>

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
static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TStoreManager::TStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* Tablet_)
    : Config_(config)
    , Tablet_(Tablet_)
    , RotationScheduled_(false)
{
    YCHECK(Config_);
    YCHECK(Tablet_);

    VersionedPooledRows_.reserve(MaxRowsPerRead);
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

    reader->ReadUnversionedRowset(&PooledKeys_);

    UnversionedPooledRows_.clear();
    
    for (auto pooledKey : PooledKeys_) {
        auto key = TOwningKey(pooledKey);
        auto keySuccessor = GetKeySuccessor(key.Get());

        // Construct readers.
        SmallVector<IVersionedReaderPtr, TypicalStoreCount> rowReaders;
        for (const auto& pair : Tablet_->Stores()) {
            const auto& store = pair.second;
            auto rowReader = store->CreateReader(
                key,
                keySuccessor,
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

        TKeyComparer keyComparer(keyColumnCount);

        TUnversionedRowMerger rowMerger(
            &LookupPool_,
            schemaColumnCount,
            keyColumnCount,
            columnFilter);

        rowMerger.Start(key.Begin());

        // Merge values.
        for (const auto& reader : rowReaders) {
            VersionedPooledRows_.clear();
            // NB: Reading at most one row.
            reader->Read(&VersionedPooledRows_);
            if (VersionedPooledRows_.empty())
                continue;

            auto partialRow = VersionedPooledRows_[0];
            if (keyComparer(key, partialRow.BeginKeys()) != 0)
                continue;

            rowMerger.AddPartialRow(partialRow);
        }

        auto mergedRow = rowMerger.BuildMergedRow(true);
        UnversionedPooledRows_.push_back(mergedRow);
    }
    
    writer->WriteUnversionedRowset(UnversionedPooledRows_);
}

void TStoreManager::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prewrite,
    std::vector<TDynamicRow>* lockedRows)
{
    CheckLockAndMaybeMigrateRow(
        transaction,
        row,
        ERowLockMode::Write);

    auto dynamicRow = Tablet_->GetActiveStore()->WriteRow(
        transaction,
        row,
        prewrite);

    if (lockedRows && dynamicRow) {
        lockedRows->push_back(dynamicRow);
    }
}

void TStoreManager::DeleteRow(
    TTransaction* transaction,
    NVersionedTableClient::TKey key,
    bool prewrite,
    std::vector<TDynamicRow>* lockedRows)
{
    CheckLockAndMaybeMigrateRow(
        transaction,
        key,
        ERowLockMode::Delete);

    auto dynamicRow = Tablet_->GetActiveStore()->DeleteRow(
        transaction,
        key,
        prewrite);

    if (lockedRows && dynamicRow) {
        lockedRows->push_back(dynamicRow);
    }
}

void TStoreManager::ConfirmRow(const TDynamicRowRef& rowRef)
{
    auto row = MaybeMigrateRow(rowRef);
    Tablet_->GetActiveStore()->ConfirmRow(row);
}

void TStoreManager::PrepareRow(const TDynamicRowRef& rowRef)
{
    auto row = MaybeMigrateRow(rowRef);
    Tablet_->GetActiveStore()->PrepareRow(row);
}

void TStoreManager::CommitRow(const TDynamicRowRef& rowRef)
{
    auto row = MaybeMigrateRow(rowRef);
    Tablet_->GetActiveStore()->CommitRow(row);
}

void TStoreManager::AbortRow(const TDynamicRowRef& rowRef)
{
    // NB: Even passive store can handle this.
    rowRef.Store->AbortRow(rowRef.Row);
    CheckForUnlockedStore(rowRef.Store);
}

TDynamicRow TStoreManager::MaybeMigrateRow(const TDynamicRowRef& rowRef)
{
    if (rowRef.Store->GetState() == EStoreState::ActiveDynamic) {
        return rowRef.Row;
    }

    auto migrateFrom = rowRef.Store;
    const auto& migrateTo = Tablet_->GetActiveStore();
    auto migratedRow = migrateFrom->MigrateRow(rowRef.Row, migrateTo);

    CheckForUnlockedStore(migrateFrom);

    return migratedRow;
}

void TStoreManager::CheckLockAndMaybeMigrateRow(
    TTransaction* transaction,
    TUnversionedRow key,
    ERowLockMode mode)
{
    for (const auto& store : LockedStores_) {
        if (store->CheckLockAndMaybeMigrateRow(
            key,
            transaction,
            ERowLockMode::Write,
            Tablet_->GetActiveStore()))
        {
            CheckForUnlockedStore(store);
            break;
        }
    }

    // TODO(babenko): check passive stores for write timestamps
}

void TStoreManager::CheckForUnlockedStore(const TDynamicMemoryStorePtr& store)
{
    if (store == Tablet_->GetActiveStore() || store->GetLockCount() > 0)
        return;

    LOG_INFO("Store unlocked and will be dropped (TabletId: %s, StoreId: %s)",
        ~ToString(Tablet_->GetId()),
        ~ToString(store->GetId()));
    YCHECK(LockedStores_.erase(store) == 1);
}

bool TStoreManager::IsRotationNeeded() const
{
    const auto& store = Tablet_->GetActiveStore();
    return
        store->GetKeyCount() >= Config_->KeyCountRotationThreshold ||
        store->GetValueCount() >= Config_->ValueCountRotationThreshold ||
        store->GetAlignedPoolSize() >= Config_->AlignedPoolSizeRotationThreshold ||
        store->GetUnalignedPoolSize() >= Config_->UnalignedPoolSizeRotationThreshold;
}

void TStoreManager::SetRotationScheduled()
{
    RotationScheduled_ = true;

    LOG_INFO("Tablet store rotation scheduled (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
}

void TStoreManager::ResetRotationScheduled()
{
    if (RotationScheduled_) {
        RotationScheduled_ = false;
        LOG_INFO("Tablet store rotation canceled (TabletId: %s)",
            ~ToString(Tablet_->GetId()));
    }
}

void TStoreManager::Rotate(bool createNew)
{
    RotationScheduled_ = false;

    auto activeStore = Tablet_->GetActiveStore();
    YCHECK(activeStore);
    activeStore->SetState(EStoreState::PassiveDynamic);

    if (activeStore->GetLockCount() > 0) {
        LOG_INFO("Current store is locked and will be kept (TabletId: %s, StoreId: %s, LockCount: %d)",
            ~ToString(Tablet_->GetId()),
            ~ToString(activeStore->GetId()),
            activeStore->GetLockCount());
        YCHECK(LockedStores_.insert(activeStore).second);
    }

    if (createNew) {
        CreateActiveStore();
    } else {
        Tablet_->SetActiveStore(nullptr);
    }

    LOG_INFO("Tablet stores rotated (TabletId: %s, StoreCount: %d)",
        ~ToString(Tablet_->GetId()),
        static_cast<int>(Tablet_->Stores().size()));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
