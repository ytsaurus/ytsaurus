#include "ordered_store_manager.h"
#include "tablet.h"
#include "store.h"
#include "transaction.h"
#include "ordered_dynamic_store.h"
#include "config.h"

#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>

namespace NYT {
namespace NTabletNode {

using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NObjectClient;

using NTabletNode::NProto::TAddStoreDescriptor;

////////////////////////////////////////////////////////////////////////////////

TOrderedStoreManager::TOrderedStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    ITabletContext* tabletContext,
    NHydra::IHydraManagerPtr hydraManager,
    TInMemoryManagerPtr inMemoryManager,
    IClientPtr client)
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
    TTimestamp /*commitTimestamp*/,
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
        rowRef.Store->CommitRow(transaction, rowRef.Row);
        CheckForUnlockedStore(rowRef.Store);
        ActiveStore_->CommitRow(transaction, migratedRow);
    }
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
        ->CreateStore(Tablet_, EStoreType::OrderedDynamic, storeId)
        ->AsOrderedDynamic();

    Tablet_->AddStore(ActiveStore_);
    Tablet_->SetActiveStore(ActiveStore_);

    LOG_INFO_UNLESS(IsRecovery(), "Active store created (StoreId: %v)",
        storeId);
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

IDynamicStore* TOrderedStoreManager::GetActiveStore() const
{
    return ActiveStore_.Get();
}

TStoreFlushCallback TOrderedStoreManager::MakeStoreFlushCallback(
    IDynamicStorePtr store,
    TTabletSnapshotPtr tabletSnapshot)
{
    auto reader = store->AsOrderedDynamic()->CreateFlushReader();

    return BIND([=, this_ = MakeStrong(this)] (ITransactionPtr transaction) {
        return std::vector<TAddStoreDescriptor>();
    });
}

void TOrderedStoreManager::ValidateOnWrite(
    const TTransactionId& transactionId,
    TUnversionedRow row)
{
    try {
        ValidateServerDataRow(row, 0, Tablet_->Schema());
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

