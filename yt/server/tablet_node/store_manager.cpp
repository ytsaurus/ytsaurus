#include "stdafx.h"
#include "store_manager.h"
#include "tablet.h"
#include "dynamic_memory_store.h"
#include "static_memory_store.h"
#include "compaction.h"
#include "config.h"

#include <core/concurrency/fiber.h>

#include <ytlib/tablet_client/protocol.h>

#include <ytlib/new_table_client/name_table.h>

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

TStoreManager::TStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* Tablet_,
    IInvokerPtr automatonInvoker,
    IInvokerPtr compactionInvoker)
    : Config_(config)
    , Tablet_(Tablet_)
    , AutomatonInvoker_(automatonInvoker)
    , CompactionInvoker_(compactionInvoker)
    , ActiveDynamicMemoryStore_(New<TDynamicMemoryStore>(
        Config_,
        Tablet_))
    , MemoryCompactor_(new TMemoryCompactor(Config_, Tablet_))
    , MemoryCompactionInProgress_(false)
{
    YCHECK(Config_);
    YCHECK(Tablet_);
    YCHECK(AutomatonInvoker_);
    YCHECK(CompactionInvoker_);
    VERIFY_INVOKER_AFFINITY(AutomatonInvoker_, AutomatonThread);
    VERIFY_INVOKER_AFFINITY(CompactionInvoker_, CompactionThread);
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

    int keyCount = static_cast<int>(Tablet_->KeyColumns().size());
    int schemaColumnCount = static_cast<int>(Tablet_->Schema().Columns().size());

    SmallVector<bool, TypicalColumnCount> columnFilterFlags(schemaColumnCount);
    if (columnFilter.All) {
        for (int id = 0; id < schemaColumnCount; ++id) {
            columnFilterFlags[id] = true;
        }
    } else {
        for (const auto& name : columnFilter.Columns) {
            auto id = NameTable_->FindId(name);
            if (id) {
                columnFilterFlags[*id] = true;
            }
        }
    }

    // TODO(babenko): replace with small vector once it supports move semantics
    std::vector<std::unique_ptr<IStoreScanner>> scanners;

    scanners.push_back(ActiveDynamicMemoryStore_->CreateScanner());
    if (PassiveDynamicMemoryStore_) {
        scanners.push_back(PassiveDynamicMemoryStore_->CreateScanner());
    }
    if (StaticMemoryStore_) {
        scanners.push_back(StaticMemoryStore_->CreateScanner());
    }

    bool keysWritten = false;
    TVersionedRowBuilder builder;

    for (const auto& scanner : scanners) {
        auto scannerTimestamp = scanner->Find(key, timestamp);
            
        if (scannerTimestamp == NullTimestamp)
            continue;
            
        if (scannerTimestamp & TombstoneTimestampMask)
            break;

        if (!keysWritten) {
            const auto keys = scanner->GetKeys();
            for (int id = 0; id < keyCount; ++id) {
                if (columnFilterFlags[id]) {
                    builder.AddValue(MakeVersionedValue(keys[id], NullTimestamp));
                }
            }
            keysWritten = true;
        }

        for (int id = keyCount; id < schemaColumnCount; ++id) {
            if (columnFilterFlags[id]) {
                auto* value = scanner->GetFixedValue(id - keyCount);
                if (value) {
                    builder.AddValue(*value);
                    columnFilterFlags[id] = false;
                }
            }
        }

        if (!(scannerTimestamp & IncrementalTimestampMask))
            break;
    }

    PooledRowset_.clear();
    if (keysWritten) {
        PooledRowset_.push_back(builder.GetRow());
    }
    writer->WriteVersionedRowset(PooledRowset_);
}

void TStoreManager::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prewrite,
    std::vector<TDynamicRow>* lockedRows)
{
    if (PassiveDynamicMemoryStore_) {
        PassiveDynamicMemoryStore_->CheckLockAndMaybeMigrateRow(
            row,
            transaction,
            ERowLockMode::Write,
            ActiveDynamicMemoryStore_);
    }

    auto dynamicRow = ActiveDynamicMemoryStore_->WriteRow(
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
    if (PassiveDynamicMemoryStore_) {
        PassiveDynamicMemoryStore_->CheckLockAndMaybeMigrateRow(
            key,
            transaction,
            ERowLockMode::Delete,
            ActiveDynamicMemoryStore_);
    }

    auto dynamicRow = ActiveDynamicMemoryStore_->DeleteRow(
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
    ActiveDynamicMemoryStore_->ConfirmRow(row);
}

void TStoreManager::PrepareRow(const TDynamicRowRef& rowRef)
{
    auto row = MigrateRowIfNeeded(rowRef);
    ActiveDynamicMemoryStore_->PrepareRow(row);
}

void TStoreManager::CommitRow(const TDynamicRowRef& rowRef)
{
    auto row = MigrateRowIfNeeded(rowRef);
    ActiveDynamicMemoryStore_->CommitRow(row);
}

void TStoreManager::AbortRow(const TDynamicRowRef& rowRef)
{
    // NB: Even passive store can handle it.
    YASSERT(rowRef.Store == ActiveDynamicMemoryStore_ ||
            rowRef.Store == PassiveDynamicMemoryStore_);
    rowRef.Store->AbortRow(rowRef.Row);
}

const TDynamicMemoryStorePtr& TStoreManager::GetActiveDynamicMemoryStore() const
{
    return ActiveDynamicMemoryStore_;
}

TDynamicRow TStoreManager::MigrateRowIfNeeded(const TDynamicRowRef& rowRef)
{
    if (rowRef.Store == ActiveDynamicMemoryStore_) {
        return rowRef.Row;
    }

    YASSERT(rowRef.Store == PassiveDynamicMemoryStore_);
    return PassiveDynamicMemoryStore_->MigrateRow(
        rowRef.Row,
        ActiveDynamicMemoryStore_);
}

bool TStoreManager::IsMemoryCompactionNeeded() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    
    if (MemoryCompactionInProgress_) {
        return false;
    }

    const auto& config = Tablet_->GetConfig();
    if (ActiveDynamicMemoryStore_->GetAllocatedValueCount() >= config->ValueCountMemoryCompactionThreshold) {
        return true;
    }
    if (ActiveDynamicMemoryStore_->GetAllocatedStringSpace() >= config->StringSpaceMemoryCompactionThreshold) {
        return true;
    }

    return false;
}

void TStoreManager::RunMemoryCompaction()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(!MemoryCompactionInProgress_);
    
    MemoryCompactionInProgress_ = true;
    
    YCHECK(PassiveDynamicMemoryStore_);
    PassiveDynamicMemoryStore_ = ActiveDynamicMemoryStore_;
    PassiveDynamicMemoryStore_->MakePassive();

    ActiveDynamicMemoryStore_ = New<TDynamicMemoryStore>(Config_, Tablet_);

    CompactionInvoker_->Invoke(
        BIND(&TStoreManager::DoMemoryCompaction, MakeStrong(this)));
}

void TStoreManager::DoMemoryCompaction()
{
    VERIFY_THREAD_AFFINITY(CompactionThread);
    YCHECK(MemoryCompactionInProgress_);

    auto compactedStore = MemoryCompactor_->Run(
        PassiveDynamicMemoryStore_,
        StaticMemoryStore_);

    SwitchTo(AutomatonInvoker_);

    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(MemoryCompactionInProgress_);

    MemoryCompactionInProgress_ = false;
    StaticMemoryStore_ = compactedStore;
    PassiveDynamicMemoryStore_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
