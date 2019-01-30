#include "replicated_store_manager.h"
#include "ordered_store_manager.h"
#include "tablet.h"
#include "private.h"

#include <yt/client/table_client/wire_protocol.h>
#include <yt/client/table_client/proto/wire_protocol.pb.h>

#include <yt/client/table_client/unversioned_row.h>

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
    , Logger(NLogging::TLogger(TabletNodeLogger)
        .AddTag("TabletId: %v, CellId: %v",
            Tablet_->GetId(),
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

void TReplicatedStoreManager::StartEpoch(TTabletSlotPtr slot)
{
    LogStoreManager_->StartEpoch(std::move(slot));
}

void TReplicatedStoreManager::StopEpoch()
{
    LogStoreManager_->StopEpoch();
}

bool TReplicatedStoreManager::ExecuteWrites(
    TWireProtocolReader* reader,
    TWriteContext* context)
{
    Y_ASSERT(context->Phase == EWritePhase::Commit);
    while (!reader->IsFinished()) {
        auto command = reader->ReadCommand();
        switch (command) {
            case EWireProtocolCommand::WriteRow: {
                auto row = reader->ReadUnversionedRow(false);
                LogStoreManager_->WriteRow(
                    BuildLogRow(row, ERowModificationType::Write),
                    context);
                break;
            }

            case EWireProtocolCommand::DeleteRow: {
                auto key = reader->ReadUnversionedRow(false);
                LogStoreManager_->WriteRow(
                    BuildLogRow(key, ERowModificationType::Delete),
                    context);
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Unsupported write command %v",
                    command);
        }
    }
    return true;
}

bool TReplicatedStoreManager::IsOverflowRotationNeeded() const
{
    return LogStoreManager_->IsOverflowRotationNeeded();
}

TError TReplicatedStoreManager::CheckOverflow() const
{
    return LogStoreManager_->CheckOverflow();
}

bool TReplicatedStoreManager::IsPeriodicRotationNeeded() const
{
    return LogStoreManager_->IsPeriodicRotationNeeded();
}

bool TReplicatedStoreManager::IsRotationPossible() const
{
    return LogStoreManager_->IsRotationPossible();
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

void TReplicatedStoreManager::ScheduleRotation()
{
    LogStoreManager_->ScheduleRotation();
}

void TReplicatedStoreManager::Rotate(bool createNewStore)
{
    LogStoreManager_->Rotate(createNewStore);
}

void TReplicatedStoreManager::AddStore(IStorePtr store, bool onMount)
{
    LogStoreManager_->AddStore(std::move(store), onMount);
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

TStoreFlushCallback  TReplicatedStoreManager::BeginStoreFlush(
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
    const std::vector<NTabletNode::NProto::TAddStoreDescriptor>& storeDescriptors)
{
    LogStoreManager_->Mount(storeDescriptors);
}

void TReplicatedStoreManager::Remount(
    TTableMountConfigPtr mountConfig,
    TTabletChunkReaderConfigPtr readerConfig,
    TTabletChunkWriterConfigPtr writerConfig,
    TTabletWriterOptionsPtr writerOptions)
{
    LogStoreManager_->Remount(
        std::move(mountConfig),
        std::move(readerConfig),
        std::move(writerConfig),
        std::move(writerOptions));
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
    const std::vector<TOwningKey>& /*pivotKeys*/)
{
    Y_UNREACHABLE();
}

void TReplicatedStoreManager::MergePartitions(
    int /*firstPartitionIndex*/,
    int /*lastPartitionIndex*/)
{
    Y_UNREACHABLE();
}

void TReplicatedStoreManager::UpdatePartitionSampleKeys(
    TPartition* /*partition*/,
    const TSharedRange<TKey>& /*keys*/)
{
    Y_UNREACHABLE();
}

TUnversionedRow TReplicatedStoreManager::BuildLogRow(
    TUnversionedRow row,
    ERowModificationType changeType)
{
    LogRowBuilder_.Reset();
    LogRowBuilder_.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 0));

    if (Tablet_->TableSchema().IsSorted()) {
        return BuildSortedLogRow(row, changeType);
    } else {
        return BuildOrderedLogRow(row, changeType);
    }
}

TUnversionedRow TReplicatedStoreManager::BuildOrderedLogRow(
    TUnversionedRow row,
    ERowModificationType changeType)
{
    YCHECK(changeType == ERowModificationType::Write);

    for (int index = 0; index < row.GetCount(); ++index) {
        auto value = row[index];
        value.Id += 1;
        LogRowBuilder_.AddValue(value);
    }
    return LogRowBuilder_.GetRow();
}

TUnversionedRow TReplicatedStoreManager::BuildSortedLogRow(
    TUnversionedRow row,
    ERowModificationType changeType)
{
    LogRowBuilder_.AddValue(MakeUnversionedInt64Value(static_cast<int>(changeType), 1));

    int keyColumnCount = Tablet_->TableSchema().GetKeyColumnCount();
    int valueColumnCount = Tablet_->TableSchema().GetValueColumnCount();

    YCHECK(row.GetCount() >= keyColumnCount);
    for (int index = 0; index < keyColumnCount; ++index) {
        auto value = row[index];
        value.Id += 2;
        LogRowBuilder_.AddValue(value);
    }

    if (changeType == ERowModificationType::Write) {
        for (int index = 0; index < valueColumnCount; ++index) {
            LogRowBuilder_.AddValue(MakeUnversionedSentinelValue(
                EValueType::Null,
                index * 2 + keyColumnCount + 2));
            LogRowBuilder_.AddValue(MakeUnversionedUint64Value(
                static_cast<ui64>(EReplicationLogDataFlags::Missing),
                index * 2 + keyColumnCount + 3));
        }
        auto logRow = LogRowBuilder_.GetRow();
        for (int index = keyColumnCount; index < row.GetCount(); ++index) {
            auto value = row[index];
            value.Id = (value.Id - keyColumnCount) * 2 + keyColumnCount + 2;
            logRow[value.Id] = value;
            auto& flags = logRow[value.Id + 1].Data.Uint64;
            flags &= ~static_cast<ui64>(EReplicationLogDataFlags::Missing);
            if (value.Aggregate) {
                flags |= static_cast<ui64>(EReplicationLogDataFlags::Aggregate);
            }
        }
    }

    return LogRowBuilder_.GetRow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
