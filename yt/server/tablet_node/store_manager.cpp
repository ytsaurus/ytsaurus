#include "stdafx.h"
#include "store_manager.h"
#include "tablet.h"
#include "dynamic_memory_store.h"
#include "config.h"
#include "private.h"

#include <core/misc/small_vector.h>

#include <core/concurrency/fiber.h>
#include <core/concurrency/parallel_collector.h>

#include <ytlib/tablet_client/protocol.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/api/transaction.h>

namespace NYT {
namespace NTabletNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NTabletClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

auto& Logger = TabletNodeLogger;

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

    Tablet_->SetActiveStore(New<TDynamicMemoryStore>(
        Config_,
        Tablet_));
}

TStoreManager::~TStoreManager()
{ }

TTablet* TStoreManager::GetTablet() const
{
    return Tablet_;
}

void TStoreManager::LookupRow(
    TTimestamp timestamp,
    NTabletClient::TProtocolReader* reader,
    NTabletClient::TProtocolWriter* writer)
{
    auto key = reader->ReadUnversionedRow();
    auto columnFilter = reader->ReadColumnFilter();

    int keyColumnCount = static_cast<int>(Tablet_->KeyColumns().size());
    int schemaColumnCount = static_cast<int>(Tablet_->Schema().Columns().size());

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

    auto keySuccessor = GetKeySuccessor(key);
    SmallVector<IVersionedReaderPtr, 16> rowReaders;
    auto tryAddStore = [&] (const IStorePtr& store) {
        auto rowReader = store->CreateReader(
            key,
            keySuccessor.Get(),
            timestamp,
            columnFilter);
        if (rowReader) {
            rowReaders.push_back(std::move(rowReader));
        }
    };

    tryAddStore(Tablet_->GetActiveStore());

    const auto& passiveStores = Tablet_->PassiveStores();
    for (auto it = passiveStores.rbegin(); it != passiveStores.rend(); ++it) {
        tryAddStore(*it);
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

    // Merge values.
    TUnversionedRowBuilder builder;
    bool keysWritten = false;
    std::vector<TVersionedRow> rows;
    TKeyPrefixComparer keyComparer(keyColumnCount);
    
    for (const auto& reader : rowReaders) {
        if (!reader->Read(&rows))
            continue;

        YASSERT(!rows.empty());
        auto row = rows[0];

        YASSERT(row.GetKeyCount() == keyColumnCount);
        const auto* rowKeys = row.BeginKeys();

        if (keyComparer(key.Begin(), row.BeginKeys()) != 0)
            continue;

        YASSERT(row.GetTimestampCount() == 1);
        auto rowTimestamp = row.BeginTimestamps()[0];

        if (rowTimestamp & TombstoneTimestampMask)
            break;

        if (!keysWritten) {
            for (int id = 0; id < keyColumnCount; ++id) {
                if (columnFilterFlags[id]) {
                    builder.AddValue(rowKeys[id]);
                    columnFilterFlags[id] = false;
                }
            }
            keysWritten = true;
        }

        const auto* rowValues = row.BeginValues();
        for (int index = 0; index < row.GetValueCount(); ++index) {
            const auto& value = rowValues[index];
            int id = value.Id;
            if (columnFilterFlags[id]) {
                builder.AddValue(value);
                columnFilterFlags[id] = false;
            }
        }

        if (!(rowTimestamp & IncrementalTimestampMask))
            break;
    }

    PooledRowset_.clear();
    if (keysWritten) {
        auto row = builder.GetRow();
        PooledRowset_.push_back(row);
    }
    writer->WriteUnversionedRowset(PooledRowset_);
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
        NameTable_,
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
    // NB: Even passive store can handle it.
    rowRef.Store->AbortRow(rowRef.Row);
    CheckForUnlockedStore(rowRef.Store);
}

TDynamicRow TStoreManager::MaybeMigrateRow(const TDynamicRowRef& rowRef)
{
    if (rowRef.Store->IsActive()) {
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

    LOG_INFO("Store unlocked and will be dropped (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
    YCHECK(LockedStores_.erase(store) == 1);
}

bool TStoreManager::IsRotationNeeded() const
{
    const auto& store = Tablet_->GetActiveStore();
    if (store->GetAllocatedValueCount() >= Config_->ValueCountRotationThreshold) {
        return true;
    }
    if (store->GetAllocatedStringSpace() >= Config_->StringSpaceRotationThreshold) {
        return true;
    }

    return false;
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

void TStoreManager::Rotate()
{
    auto activeStore = Tablet_->GetActiveStore();
    Tablet_->PassiveStores().push_back(activeStore);
    activeStore->MakePassive();

    LOG_INFO("Tablet stores rotated (TabletId: %s, StoreCount: %d)",
        ~ToString(Tablet_->GetId()),
        static_cast<int>(Tablet_->PassiveStores().size()));

    if (activeStore->GetLockCount() > 0) {
        LOG_INFO("Current store is locked and will be kept (TabletId: %s, LockCount: %d)",
            ~ToString(Tablet_->GetId()),
            activeStore->GetLockCount());
        YCHECK(LockedStores_.insert(activeStore).second);
    }

    Tablet_->SetActiveStore(New<TDynamicMemoryStore>(
        Config_,
        Tablet_));

    RotationScheduled_ = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
