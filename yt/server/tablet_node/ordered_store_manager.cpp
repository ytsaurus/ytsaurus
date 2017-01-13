#include "ordered_store_manager.h"
#include "tablet.h"
#include "store.h"
#include "transaction.h"
#include "ordered_dynamic_store.h"
#include "config.h"

#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>

#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/name_table.h>

#include <yt/ytlib/chunk_client/confirming_writer.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NApi;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NObjectClient;
using namespace NTransactionClient;

using NTabletNode::NProto::TAddStoreDescriptor;

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerFlushRead = 1024;

////////////////////////////////////////////////////////////////////////////////

TOrderedStoreManager::TOrderedStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    ITabletContext* tabletContext,
    NHydra::IHydraManagerPtr hydraManager,
    TInMemoryManagerPtr inMemoryManager,
    INativeClientPtr client)
    : TStoreManagerBase(
        std::move(config),
        tablet,
        tabletContext,
        std::move(hydraManager),
        std::move(inMemoryManager),
        std::move(client))
{
    if (Tablet_->GetActiveStore()) {
        ActiveStore_ = Tablet_->GetActiveStore()->AsOrderedDynamic();
    }
}

void TOrderedStoreManager::Mount(const std::vector<TAddStoreDescriptor>& storeDescriptors)
{
    TStoreManagerBase::Mount(storeDescriptors);

    // Compute total row row.
    if (Tablet_->StoreRowIndexMap().empty()) {
        Tablet_->SetTotalRowCount(0);
    } else {
        auto lastStore = (--Tablet_->StoreRowIndexMap().end())->second;
        Tablet_->SetTotalRowCount(lastStore->GetStartingRowIndex() + lastStore->GetRowCount());
    }
}

void TOrderedStoreManager::ExecuteAtomicWrite(
    TTransaction* transaction,
    TWireProtocolReader* reader,
    bool prelock)
{
    auto command = reader->ReadCommand();
    switch (command) {
        case EWireProtocolCommand::WriteRow: {
            auto row = reader->ReadUnversionedRow(false);
            WriteRow(
                transaction,
                row,
                prelock);
            break;
        }

        default:
            THROW_ERROR_EXCEPTION("Unsupported write command %v",
                command);
    }
}

void TOrderedStoreManager::ExecuteNonAtomicWrite(
    const TTransactionId& /*transactionId*/,
    TWireProtocolReader* /*reader*/)
{
    THROW_ERROR_EXCEPTION("Non-atomic writes to ordered tablets are not supported");
}

TOrderedDynamicRowRef TOrderedStoreManager::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prelock)
{
    if (prelock) {
        ValidateOnWrite(transaction->GetId(), row);
    }

    auto dynamicRow = ActiveStore_->WriteRow(transaction, row);
    auto dynamicRowRef = TOrderedDynamicRowRef(
        ActiveStore_.Get(),
        this,
        dynamicRow,
        Tablet_->GetCommitOrdering() == ECommitOrdering::Weak);
    LockRow(transaction, prelock, dynamicRowRef);
    return dynamicRowRef;
}

void TOrderedStoreManager::LockRow(
    TTransaction* transaction,
    bool prelock,
    const TOrderedDynamicRowRef& rowRef)
{
    if (prelock) {
        transaction->PrelockedOrderedRows().push(rowRef);
    } else {
        transaction->LockedOrderedRows().push_back(rowRef);
    }
}

void TOrderedStoreManager::ConfirmRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef)
{
    transaction->LockedOrderedRows().push_back(rowRef);
}

void TOrderedStoreManager::PrepareRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef)
{
    rowRef.Store->PrepareRow(transaction, rowRef.Row);
}

void TOrderedStoreManager::CommitRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef)
{
    if (rowRef.Store == ActiveStore_) {
        ActiveStore_->CommitRow(transaction, rowRef.Row);
    } else {
        auto migratedRow = ActiveStore_->MigrateRow(transaction, rowRef.Row);
        // NB: In contrast to ordered tablets, for ordered ones we don't commit row in
        // the original store that row has just migrated from.
        rowRef.Store->AbortRow(transaction, rowRef.Row);
        CheckForUnlockedStore(rowRef.Store);
        ActiveStore_->CommitRow(transaction, migratedRow);
    }
    Tablet_->SetTotalRowCount(Tablet_->GetTotalRowCount() + 1);
    UpdateLastCommitTimestamp(transaction->GetCommitTimestamp());
}

void TOrderedStoreManager::AbortRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef)
{
    rowRef.Store->AbortRow(transaction, rowRef.Row);
    CheckForUnlockedStore(rowRef.Store);
}

void TOrderedStoreManager::CreateActiveStore()
{
    auto storeId = TabletContext_->GenerateId(EObjectType::OrderedDynamicTabletStore);
    ActiveStore_ = TabletContext_
        ->CreateStore(Tablet_, EStoreType::OrderedDynamic, storeId, nullptr)
        ->AsOrderedDynamic();

    i64 startingRowIndex = 0;
    const auto& storeRowIndexMap = Tablet_->StoreRowIndexMap();
    if (!storeRowIndexMap.empty()) {
        const auto& lastStore = storeRowIndexMap.rbegin()->second;
        YCHECK(lastStore->GetRowCount() > 0);
        startingRowIndex = lastStore->GetStartingRowIndex() + lastStore->GetRowCount();
    }
    ActiveStore_->SetStartingRowIndex(startingRowIndex);

    Tablet_->AddStore(ActiveStore_);
    Tablet_->SetActiveStore(ActiveStore_);

    LOG_INFO_UNLESS(IsRecovery(), "Active store created (StoreId: %v, StartingRowIndex: %v)",
        storeId,
        startingRowIndex);
}

void TOrderedStoreManager::ResetActiveStore()
{
    ActiveStore_.Reset();
}

void TOrderedStoreManager::OnActiveStoreRotated()
{ }

bool TOrderedStoreManager::IsStoreCompactable(IStorePtr /*store*/) const
{
    return false;
}

bool TOrderedStoreManager::IsStoreFlushable(IStorePtr store) const
{
    if (!TStoreManagerBase::IsStoreFlushable(store)) {
        return false;
    }

    // Ensure that stores are being flushed in order.
    auto orderedStore = store->AsOrdered();
    i64 startingRowIndex = orderedStore->GetStartingRowIndex();
    const auto& rowIndexMap = store->GetTablet()->StoreRowIndexMap();
    auto it = rowIndexMap.find(startingRowIndex);
    YCHECK(it != rowIndexMap.end());
    if (it != rowIndexMap.begin() && (--it)->second->GetStoreState() != EStoreState::Persistent) {
        return false;
    }

    return true;
}

IOrderedStoreManagerPtr TOrderedStoreManager::AsOrdered()
{
    return this;
}

IDynamicStore* TOrderedStoreManager::GetActiveStore() const
{
    return ActiveStore_.Get();
}

TStoreFlushCallback TOrderedStoreManager::MakeStoreFlushCallback(
    IDynamicStorePtr store,
    TTabletSnapshotPtr tabletSnapshot)
{
    auto orderedDynamicStore = store->AsOrderedDynamic();
    auto reader = orderedDynamicStore->CreateFlushReader();

    return BIND([=, this_ = MakeStrong(this)] (ITransactionPtr transaction) {
        auto chunkWriter = CreateConfirmingWriter(
            tabletSnapshot->WriterConfig,
            tabletSnapshot->WriterOptions,
            Client_->GetNativeConnection()->GetPrimaryMasterCellTag(),
            transaction->GetId(),
            NullChunkListId,
            New<TNodeDirectory>(),
            Client_);

        TChunkTimestamps chunkTimestamps;
        chunkTimestamps.MinTimestamp = orderedDynamicStore->GetMinTimestamp();
        chunkTimestamps.MaxTimestamp = orderedDynamicStore->GetMaxTimestamp();

        auto tableWriter = CreateSchemalessChunkWriter(
            tabletSnapshot->WriterConfig,
            tabletSnapshot->WriterOptions,
            tabletSnapshot->PhysicalSchema,
            chunkWriter,
            chunkTimestamps);

        WaitFor(tableWriter->Open())
            .ThrowOnError();

        std::vector<TUnversionedRow> rows;
        rows.reserve(MaxRowsPerFlushRead);

        i64 rowCount = 0;

        while (true) {
            // NB: Memory store reader is always synchronous.
            reader->Read(&rows);
            if (rows.empty()) {
                break;
            }

            rowCount += rows.size();
            if (!tableWriter->Write(rows)) {
                WaitFor(tableWriter->GetReadyEvent())
                    .ThrowOnError();
            }
        }

        if (rowCount == 0) {
            return std::vector<TAddStoreDescriptor>();
        }

        WaitFor(tableWriter->Close())
            .ThrowOnError();

        TAddStoreDescriptor descriptor;
        descriptor.set_store_type(static_cast<int>(EStoreType::OrderedChunk));
        ToProto(descriptor.mutable_store_id(), chunkWriter->GetChunkId());
        descriptor.mutable_chunk_meta()->CopyFrom(tableWriter->GetMasterMeta());
        descriptor.set_starting_row_index(orderedDynamicStore->GetStartingRowIndex());
        return std::vector<TAddStoreDescriptor>{descriptor};
    });
}

void TOrderedStoreManager::ValidateOnWrite(
    const TTransactionId& transactionId,
    TUnversionedRow row)
{
    try {
        ValidateServerDataRow(row, Tablet_->PhysicalSchema());
    } catch (TErrorException& ex) {
        auto& errorAttributes = ex.Error().Attributes();
        errorAttributes.Set("transaction_id", transactionId);
        errorAttributes.Set("tablet_id", Tablet_->GetId());
        errorAttributes.Set("row", row);
        throw ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

