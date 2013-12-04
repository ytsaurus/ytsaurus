#include "stdafx.h"
#include "store_manager.h"
#include "tablet.h"
#include "dynamic_memory_store.h"
#include "static_memory_store.h"
#include "compaction.h"
#include "config.h"

#include <ytlib/chunk_client/memory_reader.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/writer.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/name_table.h>

#include <ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
using namespace NTransactionClient;

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

TTablet* TStoreManager::GetTablet() const
{
    return Tablet_;
}

void TStoreManager::Lookup(
    NVersionedTableClient::TKey key,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter,
    TChunkMeta* chunkMeta,
    std::vector<TSharedRef>* blocks)
{
    auto memoryWriter = New<TMemoryWriter>();

    auto chunkWriter = New<TChunkWriter>(
        New<TChunkWriterConfig>(), // TODO(babenko): make static
        New<TEncodingWriterOptions>(), // TODO(babenko): make static
        memoryWriter);

    int keyCount = static_cast<int>(Tablet_->KeyColumns().size());
    int schemaColumnCount = static_cast<int>(Tablet_->Schema().Columns().size());

    TSmallVector<int, TypicalColumnCount> fixedColumnIds(schemaColumnCount);

    auto globalNameTable = Tablet_->GetNameTable();
    TNameTablePtr localNameTable;
    if (columnFilter.All) {
        localNameTable = globalNameTable;

        for (int globalId = 0; globalId < schemaColumnCount; ++globalId) {
            fixedColumnIds[globalId] = globalId;
        }
    } else {
        localNameTable = New<TNameTable>();

        for (int globalId = 0; globalId < schemaColumnCount; ++globalId) {
            fixedColumnIds[globalId] = -1;
        }

        for (const auto& name : columnFilter.Columns) {
            auto globalId = globalNameTable->FindId(name);
            if (globalId) {
                int localId = localNameTable->GetIdOrRegisterName(name);
                if (*globalId < schemaColumnCount) {
                    fixedColumnIds[*globalId] = localId;
                }
            }
        }
    }

    chunkWriter->Open(
        std::move(localNameTable),
        Tablet_->Schema(),
        Tablet_->KeyColumns());

    // TODO(babenko): replace with small vector once we update our fork from LLVM
    std::vector<std::unique_ptr<IStoreScanner>> scanners;

    scanners.push_back(ActiveDynamicMemoryStore_->CreateScanner());
    if (PassiveDynamicMemoryStore_) {
        scanners.push_back(PassiveDynamicMemoryStore_->CreateScanner());
    }
    if (StaticMemoryStore_) {
        scanners.push_back(StaticMemoryStore_->CreateScanner());
    }

    bool keysWritten = false;

    for (const auto& scanner : scanners) {
        auto scannerTimestamp = scanner->Find(key, timestamp);
            
        if (scannerTimestamp == NullTimestamp)
            continue;
            
        if (scannerTimestamp & TombstoneTimestampMask)
            break;

        if (!keysWritten) {
            const auto keys = scanner->GetKeys();
            for (int globalId = 0; globalId < keyCount; ++globalId) {
                int localId = fixedColumnIds[globalId];
                if (localId >= 0) {
                    auto valueCopy = keys[globalId];
                    valueCopy.Id = localId;
                    chunkWriter->WriteValue(valueCopy);
                }
            }
            keysWritten = true;
        }

        for (int globalId = keyCount; globalId < schemaColumnCount; ++globalId) {
            int localId = fixedColumnIds[globalId];
            if (localId >= 0) {
                auto* value = scanner->GetFixedValue(globalId - keyCount);
                if (value) {
                    auto valueCopy = *value;
                    valueCopy.Id = localId;
                    chunkWriter->WriteValue(valueCopy);
                    fixedColumnIds[globalId] = -1;
                }
            }
        }

        if (!(scannerTimestamp & IncrementalTimestampMask))
            break;
    }

    if (keysWritten) {
        for (int globalId = keyCount; globalId < schemaColumnCount; ++globalId) {
            int localId = fixedColumnIds[globalId];
            if (localId >= 0) {
                chunkWriter->WriteValue(MakeUnversionedSentinelValue(EValueType::Null, localId));
            }
        }

        chunkWriter->EndRow();
    }

    // NB: The writer must be synchronous.
    YCHECK(chunkWriter->AsyncClose().Get().IsOK());

    *chunkMeta = std::move(memoryWriter->GetChunkMeta());
    *blocks = std::move(memoryWriter->GetBlocks());
}

void TStoreManager::Write(
    TTransaction* transaction,
    TChunkMeta chunkMeta,
    std::vector<TSharedRef> blocks,
    bool prewrite,
    std::vector<TDynamicRow>* lockedRows)
{
    auto memoryReader = New<TMemoryReader>(
        std::move(chunkMeta),
        std::move(blocks));

    auto chunkReader = CreateChunkReader(
        New<TChunkReaderConfig>(), // TODO(babenko): make configurable or cache this at least
        memoryReader);

    auto nameTable = New<TNameTable>();

    {
        // The reader is typically synchronous.
        auto error = chunkReader->Open(
            nameTable,
            Tablet_->Schema(),
            true).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    const int RowsBufferSize = 1000;
    // TODO(babenko): use unversioned rows
    std::vector<TVersionedRow> rows;
    rows.reserve(RowsBufferSize);

    while (true) {
        bool hasData = chunkReader->Read(&rows);
        
        for (auto row : rows) {
            auto bucket = ActiveDynamicMemoryStore_->WriteRow(
                nameTable,
                transaction,
                row,
                prewrite);
            lockedRows->push_back(bucket);
        }

        if (!hasData) {
            break;
        }
        
        if (rows.size() < rows.capacity()) {
            // The reader is typically synchronous.
            auto result = chunkReader->GetReadyEvent().Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
        
        rows.clear();
    }
}

void TStoreManager::Delete(
    TTransaction* transaction,
    const std::vector<NVersionedTableClient::TOwningKey>& keys,
    bool predelete,
    std::vector<TDynamicRow>* lockedRows)
{
    for (auto key : keys) {
        auto bucket = ActiveDynamicMemoryStore_->DeleteRow(
            transaction,
            key,
            predelete);
        lockedRows->push_back(bucket);
    }
}

void TStoreManager::ConfirmRow(TDynamicRow row)
{
    ActiveDynamicMemoryStore_->ConfirmRow(row);
}

void TStoreManager::PrepareRow(TDynamicRow row)
{
    ActiveDynamicMemoryStore_->PrepareRow(row);
}

void TStoreManager::CommitRow(TDynamicRow row)
{
    ActiveDynamicMemoryStore_->CommitRow(row);
}

void TStoreManager::AbortRow(TDynamicRow row)
{
    ActiveDynamicMemoryStore_->AbortRow(row);
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
    ActiveDynamicMemoryStore_ = New<TDynamicMemoryStore>(Config_, Tablet_);

    CompactionInvoker_->Invoke(
        BIND(&TStoreManager::DoMemoryCompaction, MakeStrong(this)));
}

void TStoreManager::DoMemoryCompaction()
{
    VERIFY_THREAD_AFFINITY(CompactionThread);
    YCHECK(MemoryCompactionInProgress_);

    auto compactedStore = MemoryCompactor_->Run(PassiveDynamicMemoryStore_, StaticMemoryStore_);

    AutomatonInvoker_->Invoke(
        BIND(&TStoreManager::FinishMemoryCompaction, MakeStrong(this), compactedStore));
}

void TStoreManager::FinishMemoryCompaction(TStaticMemoryStorePtr compactedStore)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(MemoryCompactionInProgress_);

    MemoryCompactionInProgress_ = false;
    StaticMemoryStore_ = compactedStore;
    PassiveDynamicMemoryStore_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
