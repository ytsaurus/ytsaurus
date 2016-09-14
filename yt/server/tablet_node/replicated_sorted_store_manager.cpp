#include "replicated_sorted_store_manager.h"
#include "ordered_store_manager.h"
#include "tablet.h"
#include "private.h"

#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>

#include <yt/ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NTabletNode {

using namespace NApi;
using namespace NHydra;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TReplicatedSortedStoreManager::TReplicatedSortedStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    ITabletContext* tabletContext,
    NHydra::IHydraManagerPtr hydraManager,
    TInMemoryManagerPtr inMemoryManager,
    INativeClientPtr client)
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
    , Underlying_(New<TOrderedStoreManager>(
        Config_,
        Tablet_,
        TabletContext_,
        HydraManager_,
        InMemoryManager_,
        Client_))
{ }

TTablet* TReplicatedSortedStoreManager::GetTablet() const
{
    return Tablet_;
}

bool TReplicatedSortedStoreManager::HasActiveLocks() const
{
    return Underlying_->HasActiveLocks();
}

bool TReplicatedSortedStoreManager::HasUnflushedStores() const
{
    return Underlying_->HasUnflushedStores();
}

void TReplicatedSortedStoreManager::StartEpoch(TTabletSlotPtr slot)
{
    Underlying_->StartEpoch(std::move(slot));
}

void TReplicatedSortedStoreManager::StopEpoch()
{
    Underlying_->StopEpoch();
}

void TReplicatedSortedStoreManager::ExecuteAtomicWrite(
    TTransaction* transaction,
    TWireProtocolReader* reader,
    bool prelock)
{
    auto command = reader->ReadCommand();
    switch (command) {
        case EWireProtocolCommand::WriteRow: {
            auto row = reader->ReadUnversionedRow();
            WriteRow(
                transaction,
                row,
                prelock);
            break;
        }

        case EWireProtocolCommand::DeleteRow: {
            auto key = reader->ReadUnversionedRow();
            DeleteRow(
                transaction,
                key,
                prelock);
            break;
        }

        default:
            THROW_ERROR_EXCEPTION("Unsupported write command %v",
                command);
    }
}

void TReplicatedSortedStoreManager::ExecuteNonAtomicWrite(
    const TTransactionId& /*transactionId*/,
    TWireProtocolReader* /*reader*/)
{
    THROW_ERROR_EXCEPTION("Non-atomic writes to replicated tables are not supported");
}

bool TReplicatedSortedStoreManager::IsOverflowRotationNeeded() const
{
    return Underlying_->IsOverflowRotationNeeded();
}

bool TReplicatedSortedStoreManager::IsPeriodicRotationNeeded() const
{
    return Underlying_->IsPeriodicRotationNeeded();
}

bool TReplicatedSortedStoreManager::IsRotationPossible() const
{
    return Underlying_->IsRotationPossible();
}

bool TReplicatedSortedStoreManager::IsForcedRotationPossible() const
{
    return Underlying_->IsForcedRotationPossible();
}

bool TReplicatedSortedStoreManager::IsRotationScheduled() const
{
    return Underlying_->IsRotationScheduled();
}

void TReplicatedSortedStoreManager::ScheduleRotation()
{
    Underlying_->ScheduleRotation();
}

void TReplicatedSortedStoreManager::Rotate(bool createNewStore)
{
    Underlying_->Rotate(createNewStore);
}

void TReplicatedSortedStoreManager::AddStore(IStorePtr store, bool onMount)
{
    Underlying_->AddStore(std::move(store), onMount);
}

void TReplicatedSortedStoreManager::RemoveStore(IStorePtr store)
{
    Underlying_->RemoveStore(std::move(store));
}

void TReplicatedSortedStoreManager::BackoffStoreRemoval(IStorePtr store)
{
    Underlying_->BackoffStoreRemoval(std::move(store));
}

bool TReplicatedSortedStoreManager::IsStoreLocked(IStorePtr store) const
{
    return Underlying_->IsStoreLocked(std::move(store));
}

std::vector<IStorePtr> TReplicatedSortedStoreManager::GetLockedStores() const
{
    return Underlying_->GetLockedStores();
}

IChunkStorePtr TReplicatedSortedStoreManager::PeekStoreForPreload()
{
    return NYT::NTabletNode::IChunkStorePtr();
}

void TReplicatedSortedStoreManager::BeginStorePreload(IChunkStorePtr store, TCallback<TFuture<void>()> callbackFuture)
{
    Underlying_->BeginStorePreload(std::move(store), std::move(callbackFuture));
}

void TReplicatedSortedStoreManager::EndStorePreload(IChunkStorePtr store)
{
    Underlying_->EndStorePreload(std::move(store));
}

void TReplicatedSortedStoreManager::BackoffStorePreload(IChunkStorePtr store)
{
    Underlying_->BackoffStorePreload(std::move(store));
}

bool TReplicatedSortedStoreManager::IsStoreFlushable(IStorePtr store) const
{
    return Underlying_->IsStoreFlushable(std::move(store));
}

TStoreFlushCallback  TReplicatedSortedStoreManager::BeginStoreFlush(
    IDynamicStorePtr store,
    TTabletSnapshotPtr tabletSnapshot)
{
    return Underlying_->BeginStoreFlush(std::move(store), std::move(tabletSnapshot));
}

void TReplicatedSortedStoreManager::EndStoreFlush(IDynamicStorePtr store)
{
    Underlying_->EndStoreFlush(std::move(store));
}

void TReplicatedSortedStoreManager::BackoffStoreFlush(IDynamicStorePtr store)
{
    Underlying_->BackoffStoreFlush(std::move(store));
}

bool TReplicatedSortedStoreManager::IsStoreCompactable(IStorePtr store) const
{
    return Underlying_->IsStoreCompactable(std::move(store));
}

void TReplicatedSortedStoreManager::BeginStoreCompaction(IChunkStorePtr store)
{
    Underlying_->BeginStoreCompaction(std::move(store));
}

void TReplicatedSortedStoreManager::EndStoreCompaction(IChunkStorePtr store)
{
    Underlying_->EndStoreCompaction(std::move(store));
}

void TReplicatedSortedStoreManager::BackoffStoreCompaction(IChunkStorePtr store)
{
    Underlying_->BackoffStoreCompaction(std::move(store));
}

void TReplicatedSortedStoreManager::Mount(
    const std::vector<NTabletNode::NProto::TAddStoreDescriptor>& storeDescriptors)
{
    Underlying_->Mount(storeDescriptors);
}

void TReplicatedSortedStoreManager::Remount(
    TTableMountConfigPtr mountConfig,
    TTabletWriterOptionsPtr writerOptions)
{
    Underlying_->Remount(std::move(mountConfig), std::move(writerOptions));
}

ISortedStoreManagerPtr TReplicatedSortedStoreManager::AsSorted()
{
    return this;
}

IOrderedStoreManagerPtr TReplicatedSortedStoreManager::AsOrdered()
{
    return Underlying_;
}

bool TReplicatedSortedStoreManager::SplitPartition(
    int /*partitionIndex*/,
    const std::vector<TOwningKey>& /*pivotKeys*/)
{
    Y_UNREACHABLE();
}

void TReplicatedSortedStoreManager::MergePartitions(
    int /*firstPartitionIndex*/,
    int /*lastPartitionIndex*/)
{
    Y_UNREACHABLE();
}

void TReplicatedSortedStoreManager::UpdatePartitionSampleKeys(
    TPartition* /*partition*/,
    const std::vector<TOwningKey>& /*keys*/)
{
    Y_UNREACHABLE();
}

TOrderedDynamicRowRef  TReplicatedSortedStoreManager::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prelock)
{
    return Underlying_->WriteRow(
        transaction,
        BuildLogRow(row, ERowModificationType::Write),
        prelock);
}

TOrderedDynamicRowRef TReplicatedSortedStoreManager::DeleteRow(
    TTransaction* transaction,
    TKey key,
    bool prelock)
{
    return Underlying_->WriteRow(
        transaction,
        BuildLogRow(key, ERowModificationType::Delete),
        prelock);
}

TUnversionedRow TReplicatedSortedStoreManager::BuildLogRow(
    TUnversionedRow row,
    ERowModificationType changeType)
{
    LogRowBuilder_.Reset();
    LogRowBuilder_.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 0));
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
            logRow[value.Id + 1].Data.Uint64 &= ~static_cast<ui64>(EReplicationLogDataFlags::Missing);
        }
    }

    return LogRowBuilder_.GetRow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
