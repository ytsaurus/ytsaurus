#include "replicated_store_manager.h"

#include "ordered_store_manager.h"
#include "private.h"
#include "replication_log.h"
#include "tablet.h"

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NHydra;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TReplicatedStoreManager::TReplicatedStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    ITabletContext* tabletContext,
    NHydra::IHydraManagerPtr hydraManager,
    IInMemoryManagerPtr inMemoryManager,
    NNative::IClientPtr client)
    : Config_(config)
    , Tablet_(tablet)
    , TabletContext_(tabletContext)
    , HydraManager_(std::move(hydraManager))
    , InMemoryManager_(std::move(inMemoryManager))
    , Client_(std::move(client))
    , Logger(TabletNodeLogger().WithTag("%v, CellId: %v",
        Tablet_->GetLoggingTag(),
        TabletContext_->GetCellId()))
    , LogStoreManager_(New<TOrderedStoreManager>(
        Config_,
        Tablet_,
        TabletContext_,
        HydraManager_,
        InMemoryManager_,
        Client_))
{ }

bool TReplicatedStoreManager::HasActiveLocks() const
{
    return LogStoreManager_->HasActiveLocks();
}

bool TReplicatedStoreManager::HasUnflushedStores() const
{
    return LogStoreManager_->HasUnflushedStores();
}

void TReplicatedStoreManager::StartEpoch(ITabletSlotPtr slot)
{
    LogStoreManager_->StartEpoch(std::move(slot));
}

void TReplicatedStoreManager::StopEpoch()
{
    LogStoreManager_->StopEpoch();
}

bool TReplicatedStoreManager::ExecuteWrites(
    IWireProtocolReader* reader,
    TWriteContext* context)
{
    auto tableSchema = Tablet_->GetTableSchema();

    LogStoreManager_->LockHunkStores(context);

    YT_ASSERT(context->Phase == EWritePhase::Commit);
    while (!reader->IsFinished()) {
        auto modifyRow = [&] (
            TUnversionedRow row,
            ERowModificationType modificationType)
        {
            LogStoreManager_->WriteRow(
                BuildLogRow(row, modificationType, tableSchema, &LogRowBuilder_),
                context);
        };

        auto command = reader->ReadWriteCommand(
            Tablet_->TableSchemaData(),
            /*captureValues*/ false);

        Visit(command,
            [&] (const TWriteRowCommand& command) { modifyRow(command.Row, ERowModificationType::Write); },
            [&] (const TDeleteRowCommand& command) { modifyRow(command.Row, ERowModificationType::Delete); },
            [&] (const TWriteAndLockRowCommand& command) { modifyRow(command.Row, ERowModificationType::Write); },
            [&] (const TVersionedWriteRowCommand& command) {
                LogStoreManager_->WriteRow(
                    BuildLogRow(command.VersionedRow, tableSchema, &LogRowBuilder_),
                    context);
            },
            [&] (const auto& command) {
                THROW_ERROR_EXCEPTION("Unsupported write command %v",
                    GetWireProtocolCommand(command));
            });
    }
    return true;
}

void TReplicatedStoreManager::UpdateCommittedStoreRowCount()
{
    LogStoreManager_->UpdateCommittedStoreRowCount();
}

bool TReplicatedStoreManager::IsOverflowRotationNeeded() const
{
    return LogStoreManager_->IsOverflowRotationNeeded();
}

TError TReplicatedStoreManager::CheckOverflow() const
{
    return LogStoreManager_->CheckOverflow();
}

bool TReplicatedStoreManager::IsRotationPossible() const
{
    return LogStoreManager_->IsRotationPossible();
}

std::optional<TInstant> TReplicatedStoreManager::GetLastPeriodicRotationTime() const
{
    return LogStoreManager_->GetLastPeriodicRotationTime();
}

void TReplicatedStoreManager::SetLastPeriodicRotationTime(TInstant value)
{
    LogStoreManager_->SetLastPeriodicRotationTime(value);
}

bool TReplicatedStoreManager::IsForcedRotationPossible() const
{
    return LogStoreManager_->IsForcedRotationPossible();
}

bool TReplicatedStoreManager::IsRotationScheduled() const
{
    return LogStoreManager_->IsRotationScheduled();
}

bool TReplicatedStoreManager::IsFlushNeeded() const
{
    return LogStoreManager_->IsFlushNeeded();
}

void TReplicatedStoreManager::InitializeRotation()
{
    LogStoreManager_->InitializeRotation();
}

void TReplicatedStoreManager::ScheduleRotation(NLsm::EStoreRotationReason reason)
{
    LogStoreManager_->ScheduleRotation(reason);
}

void TReplicatedStoreManager::UnscheduleRotation()
{
    LogStoreManager_->UnscheduleRotation();
}

void TReplicatedStoreManager::Rotate(bool createNewStore, NLsm::EStoreRotationReason reason, bool allowEmptyStore)
{
    LogStoreManager_->Rotate(createNewStore, reason, allowEmptyStore);
}

void TReplicatedStoreManager::AddStore(IStorePtr store, bool onMount, bool onFlush, TPartitionId partitionIdHint)
{
    LogStoreManager_->AddStore(std::move(store), onMount, onFlush, partitionIdHint);
}

void TReplicatedStoreManager::BulkAddStores(TRange<IStorePtr> /*stores*/, bool /*onMount*/)
{
    YT_ABORT();
}

void TReplicatedStoreManager::CreateActiveStore(TDynamicStoreId /*hintId*/)
{
    YT_ABORT();
}

void TReplicatedStoreManager::DiscardAllStores()
{
    YT_ABORT();
}

void TReplicatedStoreManager::RemoveStore(IStorePtr store)
{
    LogStoreManager_->RemoveStore(std::move(store));
}

void TReplicatedStoreManager::BackoffStoreRemoval(IStorePtr store)
{
    LogStoreManager_->BackoffStoreRemoval(std::move(store));
}

bool TReplicatedStoreManager::IsStoreLocked(IStorePtr store) const
{
    return LogStoreManager_->IsStoreLocked(std::move(store));
}

std::vector<IStorePtr> TReplicatedStoreManager::GetLockedStores() const
{
    return LogStoreManager_->GetLockedStores();
}

IChunkStorePtr TReplicatedStoreManager::PeekStoreForPreload()
{
    return NYT::NTabletNode::IChunkStorePtr();
}

void TReplicatedStoreManager::BeginStorePreload(IChunkStorePtr store, TCallback<TFuture<void>()> callbackFuture)
{
    LogStoreManager_->BeginStorePreload(std::move(store), std::move(callbackFuture));
}

void TReplicatedStoreManager::EndStorePreload(IChunkStorePtr store)
{
    LogStoreManager_->EndStorePreload(std::move(store));
}

void TReplicatedStoreManager::BackoffStorePreload(IChunkStorePtr store)
{
    LogStoreManager_->BackoffStorePreload(std::move(store));
}

EInMemoryMode TReplicatedStoreManager::GetInMemoryMode() const
{
    return LogStoreManager_->GetInMemoryMode();
}

bool TReplicatedStoreManager::IsStoreFlushable(IStorePtr store) const
{
    return LogStoreManager_->IsStoreFlushable(std::move(store));
}

TStoreFlushCallback TReplicatedStoreManager::BeginStoreFlush(
    IDynamicStorePtr store,
    TTabletSnapshotPtr tabletSnapshot,
    bool isUnmountWorkflow)
{
    return LogStoreManager_->BeginStoreFlush(std::move(store), std::move(tabletSnapshot), isUnmountWorkflow);
}

void TReplicatedStoreManager::EndStoreFlush(IDynamicStorePtr store)
{
    LogStoreManager_->EndStoreFlush(std::move(store));
}

void TReplicatedStoreManager::BackoffStoreFlush(IDynamicStorePtr store)
{
    LogStoreManager_->BackoffStoreFlush(std::move(store));
}

bool TReplicatedStoreManager::IsStoreCompactable(IStorePtr store) const
{
    return LogStoreManager_->IsStoreCompactable(std::move(store));
}

void TReplicatedStoreManager::BeginStoreCompaction(IChunkStorePtr store)
{
    LogStoreManager_->BeginStoreCompaction(std::move(store));
}

void TReplicatedStoreManager::EndStoreCompaction(IChunkStorePtr store)
{
    LogStoreManager_->EndStoreCompaction(std::move(store));
}

void TReplicatedStoreManager::BackoffStoreCompaction(IChunkStorePtr store)
{
    LogStoreManager_->BackoffStoreCompaction(std::move(store));
}

void TReplicatedStoreManager::Mount(
    TRange<const NTabletNode::NProto::TAddStoreDescriptor*> storeDescriptors,
    TRange<const NTabletNode::NProto::TAddHunkChunkDescriptor*> hunkChunkDescriptors,
    bool createDynamicStore,
    const NTabletNode::NProto::TMountHint& mountHint)
{
    LogStoreManager_->Mount(
        storeDescriptors,
        hunkChunkDescriptors,
        createDynamicStore,
        mountHint);
}

void TReplicatedStoreManager::Remount(const TTableSettings& settings)
{
    LogStoreManager_->Remount(settings);
}

ISortedStoreManagerPtr TReplicatedStoreManager::AsSorted()
{
    return this;
}

IOrderedStoreManagerPtr TReplicatedStoreManager::AsOrdered()
{
    return LogStoreManager_;
}

bool TReplicatedStoreManager::SplitPartition(
    int /*partitionIndex*/,
    const std::vector<TLegacyOwningKey>& /*pivotKeys*/)
{
    YT_ABORT();
}

void TReplicatedStoreManager::MergePartitions(
    int /*firstPartitionIndex*/,
    int /*lastPartitionIndex*/)
{
    YT_ABORT();
}

void TReplicatedStoreManager::UpdatePartitionSampleKeys(
    TPartition* /*partition*/,
    const TSharedRange<TLegacyKey>& /*keys*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
