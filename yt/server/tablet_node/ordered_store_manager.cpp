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

#include <yt/ytlib/chunk_client/chunk_spec.pb.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NObjectClient;

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

void TOrderedStoreManager::ExecuteAtomicWrite(
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
    TTablet* /*tablet*/,
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
    auto dynamicRowRef = TOrderedDynamicRowRef(ActiveStore_.Get(), this, dynamicRow);
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
        auto writer = CreateSchemalessMultiChunkWriter(
            Config_->ChunkWriter,
            tabletSnapshot->WriterOptions,
            TNameTable::FromSchema(tabletSnapshot->TableSchema),
            TKeyColumns(),
            TOwningKey(),
            Client_,
            Client_->GetNativeConnection()->GetPrimaryMasterCellTag(),
            transaction->GetId());

        WaitFor(writer->Open())
            .ThrowOnError();

        std::vector<TUnversionedRow> rows;
        rows.reserve(MaxRowsPerFlushRead);

        while (true) {
            // NB: Memory store reader is always synchronous.
            reader->Read(&rows);
            if (rows.empty()) {
                break;
            }
            if (!writer->Write(rows)) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }
        }

        WaitFor(writer->Close())
            .ThrowOnError();

        std::vector<TAddStoreDescriptor> result;
        i64 startingRowIndex = orderedDynamicStore->GetStartingRowIndex();
        for (const auto& chunkSpec : writer->GetWrittenChunksMasterMeta()) {
            TAddStoreDescriptor descriptor;
            descriptor.set_store_type(static_cast<int>(EStoreType::OrderedChunk));
            descriptor.mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
            descriptor.mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
            descriptor.set_starting_row_index(startingRowIndex);
            result.push_back(descriptor);
            auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());
            startingRowIndex += miscExt.row_count();
        }
        return result;
    });
}

void TOrderedStoreManager::ValidateOnWrite(
    const TTransactionId& transactionId,
    TUnversionedRow row)
{
    try {
        ValidateServerDataRow(row, Tablet_->Schema());
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

