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
    TTablet* Tablet_,
    IInvokerPtr automatonInvoker,
    IInvokerPtr compactionInvoker)
    : Config_(config)
    , Tablet_(Tablet_)
    , AutomatonInvoker_(automatonInvoker)
    , CompactionInvoker_(compactionInvoker)
    , ActiveStore_(New<TDynamicMemoryStore>(
        Config_,
        Tablet_))
    , RotationScheduled_(false)
{
    YCHECK(Config_);
    YCHECK(Tablet_);
    YCHECK(AutomatonInvoker_);
    YCHECK(CompactionInvoker_);
    VERIFY_INVOKER_AFFINITY(AutomatonInvoker_, AutomatonThread);
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
    auto tryAddStore = [&](const IStorePtr& store) {
        auto rowReader = store->CreateReader(
            key,
            keySuccessor.Get(),
            timestamp,
            columnFilter);
        if (rowReader) {
            rowReaders.push_back(std::move(rowReader));
        }
    };

    tryAddStore(ActiveStore_);
    for (auto it = PassiveStores_.rbegin(); it != PassiveStores_.rend(); ++it) {
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
    //if (PassiveDynamicMemoryStore_) {
    //    PassiveDynamicMemoryStore_->CheckLockAndMaybeMigrateRow(
    //        row,
    //        transaction,
    //        ERowLockMode::Write,
    //        ActiveStore_);
    //}

    auto dynamicRow = ActiveStore_->WriteRow(
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
    //if (PassiveDynamicMemoryStore_) {
    //    PassiveDynamicMemoryStore_->CheckLockAndMaybeMigrateRow(
    //        key,
    //        transaction,
    //        ERowLockMode::Delete,
    //        ActiveStore_);
    //}

    auto dynamicRow = ActiveStore_->DeleteRow(
        transaction,
        key,
        prewrite);
    if (lockedRows && dynamicRow) {
        lockedRows->push_back(dynamicRow);
    }
}

void TStoreManager::ConfirmRow(const TDynamicRowRef& rowRef)
{
    auto row = MigrateRowIfNeeded(rowRef);
    ActiveStore_->ConfirmRow(row);
}

void TStoreManager::PrepareRow(const TDynamicRowRef& rowRef)
{
    auto row = MigrateRowIfNeeded(rowRef);
    ActiveStore_->PrepareRow(row);
}

void TStoreManager::CommitRow(const TDynamicRowRef& rowRef)
{
    auto row = MigrateRowIfNeeded(rowRef);
    ActiveStore_->CommitRow(row);
}

void TStoreManager::AbortRow(const TDynamicRowRef& rowRef)
{
    // NB: Even passive store can handle it.
    rowRef.Store->AbortRow(rowRef.Row);
}

const TDynamicMemoryStorePtr& TStoreManager::GetActiveStore() const
{
    return ActiveStore_;
}

TDynamicRow TStoreManager::MigrateRowIfNeeded(const TDynamicRowRef& rowRef)
{
    if (rowRef.Store == ActiveStore_) {
        return rowRef.Row;
    }

    return rowRef.Store->MigrateRow(
        rowRef.Row,
        ActiveStore_);
}

bool TStoreManager::IsRotationNeeded() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& config = Tablet_->GetConfig();
    if (ActiveStore_->GetAllocatedValueCount() >= config->ValueCountRotationThreshold) {
        return true;
    }
    if (ActiveStore_->GetAllocatedStringSpace() >= config->StringSpaceRotationThreshold) {
        return true;
    }

    return false;
}

void TStoreManager::SetRotationScheduled()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    RotationScheduled_ = true;

    LOG_INFO("Tablet store rotation scheduled (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
}

void TStoreManager::ResetRotationScheduled()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (RotationScheduled_) {
        RotationScheduled_ = false;
        LOG_INFO("Tablet store rotation canceled (TabletId: %s)",
            ~ToString(Tablet_->GetId()));
    }
}

void TStoreManager::Rotate()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(RotationScheduled_);

    PassiveStores_.push_back(ActiveStore_);
    ActiveStore_ = New<TDynamicMemoryStore>(
        Config_,
        Tablet_);

    RotationScheduled_ = false;

    LOG_INFO("Tablet store rotated (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
