#include "tablet.h"

#include "tablet_cell.h"
#include "table_replica.h"
#include "tablet_action.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_tree_traverser.h>
#include <yt/yt/server/master/chunk_server/medium_base.h>
#include <yt/yt/server/master/chunk_server/dynamic_store.h>

#include <yt/yt/server/lib/tablet_server/proto/backup_manager.pb.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/proto/heartbeat.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NTableClient;
using namespace NCypressClient;
using namespace NTableServer;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

void TTableReplicaInfo::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, State_);
    PHOENIX_REGISTER_FIELD(2, CommittedReplicationRowIndex_);
    PHOENIX_REGISTER_FIELD(3, CurrentReplicationTimestamp_);
    PHOENIX_REGISTER_FIELD(4, HasError_);
}

PHOENIX_DEFINE_TYPE(TTableReplicaInfo);

void TTableReplicaInfo::Populate(
    NTabletClient::NProto::TTableReplicaStatistics* statistics) const
{
    statistics->set_committed_replication_row_index(GetCommittedReplicationRowIndex());
    statistics->set_current_replication_timestamp(ToProto(GetCurrentReplicationTimestamp()));
}

void TTableReplicaInfo::MergeFrom(
    const NTabletClient::NProto::TTableReplicaStatistics& statistics)
{
    // Updates may be reordered but we can rely on monotonicity here.
    SetCommittedReplicationRowIndex(std::max(
        GetCommittedReplicationRowIndex(),
        statistics.committed_replication_row_index()));
    SetCurrentReplicationTimestamp(std::max(
        GetCurrentReplicationTimestamp(),
        FromProto<NTransactionClient::TTimestamp>(statistics.current_replication_timestamp())));
}

////////////////////////////////////////////////////////////////////////////////

void TBackupCutoffDescriptor::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, CutoffRowIndex);
    PHOENIX_REGISTER_FIELD(2, NextDynamicStoreId);
    PHOENIX_REGISTER_FIELD(3, DynamicStoreIdsToKeep);
}

PHOENIX_DEFINE_TYPE(TBackupCutoffDescriptor);

void FormatValue(TStringBuilderBase* builder, const TBackupCutoffDescriptor& descriptor, TStringBuf spec)
{
    // TODO(arkady-e1ppa): We format descriptor twice
    // (serialization and then with respect to spec)
    // Not sure if it is required -- maybe we are free
    // to ignore the spec. Check later and remove
    // the second step if redundant.
    FormatValue(
        builder,
        Format(
            "{%v/%v/%v}",
            descriptor.CutoffRowIndex,
            descriptor.NextDynamicStoreId,
            descriptor.DynamicStoreIdsToKeep),
        spec);
}

void FromProto(
    TBackupCutoffDescriptor* descriptor,
    const NProto::TBackupCutoffDescriptor& protoDescriptor)
{
    if (protoDescriptor.has_row_index_descriptor()) {
        const auto& rowIndexDescriptor = protoDescriptor.row_index_descriptor();
        descriptor->CutoffRowIndex = rowIndexDescriptor.cutoff_row_index();
        FromProto(&descriptor->NextDynamicStoreId, rowIndexDescriptor.next_dynamic_store_id());
    } else if (protoDescriptor.has_dynamic_store_list_descriptor()) {
        const auto& dynamicStoreListDescriptor = protoDescriptor.dynamic_store_list_descriptor();
        FromProto(
            &descriptor->DynamicStoreIdsToKeep,
            dynamicStoreListDescriptor.dynamic_store_ids_to_keep());
    }
}

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(TTabletId id)
    : TTabletBase(id)
{ }

std::string TTablet::GetLowercaseObjectName() const
{
    return Format("tablet %v", GetId());
}

std::string TTablet::GetCapitalizedObjectName() const
{
    return Format("Tablet %v", GetId());
}

TYPath TTablet::GetObjectPath() const
{
    return Format("//sys/tablets/%v", GetId());
}

void TTablet::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTabletBase>();

    PHOENIX_REGISTER_FIELD(1, PivotKey_);
    PHOENIX_REGISTER_FIELD(2, NodeStatistics_);
    PHOENIX_REGISTER_FIELD(3, AuxiliaryNodeStatistics_);
    // COMPAT(atalmenev)
    PHOENIX_REGISTER_FIELD(4, OriginatorTablets_,
        .SinceVersion(EMasterReign::SaveOriginatorTabletsAfterReshard));
    PHOENIX_REGISTER_FIELD(5, TrimmedRowCount_);
    PHOENIX_REGISTER_FIELD(6, Replicas_);
    PHOENIX_REGISTER_FIELD(7, RetainedTimestamp_);
    PHOENIX_REGISTER_FIELD(8, ConflictHorizonTimestamp_,
        .SinceVersion(EMasterReign::AddPerTabletConflictHorizonTimestamp)
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            this_->ConflictHorizonTimestamp_ = MinTimestamp;
        }));
    PHOENIX_REGISTER_FIELD(9, ReplicationErrorCount_);
    PHOENIX_REGISTER_FIELD(10, UnconfirmedDynamicTableLocks_);
    PHOENIX_REGISTER_FIELD(11, EdenStoreIds_);
    PHOENIX_REGISTER_FIELD(12, BackupState_);
    PHOENIX_REGISTER_FIELD(13, BackupCutoffDescriptor_);
    PHOENIX_REGISTER_FIELD(14, BackedUpReplicaInfos_);
    PHOENIX_REGISTER_FIELD(15, DynamicStores_);
    PHOENIX_REGISTER_FIELD(16, ReplicationProgress_);
    PHOENIX_REGISTER_FIELD(17, ProlongedChunks_,
        .SinceVersion(EMasterReign::DynamicTableChunkProlongation));
}

PHOENIX_DEFINE_TYPE(TTablet);

TTableNode* TTablet::GetTable() const
{
    if (auto* owner = GetOwner()) {
        YT_VERIFY(IsTableType(owner->GetType()));
        return owner->As<TTableNode>();
    } else {
        return nullptr;
    }
}

void TTablet::SetOwner(TTabletOwnerBase* owner)
{
    if (auto* table = GetTable()) {
        YT_VERIFY(table->GetTrunkNode()->TabletCountByBackupState()[BackupState_] > 0);
        --table->GetTrunkNode()->MutableTabletCountByBackupState()[BackupState_];
    }

    if (owner) {
        YT_VERIFY(IsTableType(owner->GetType()));

        auto* table = owner->As<TTableNode>();
        YT_VERIFY(table->IsTrunk());
        ++table->MutableTabletCountByBackupState()[BackupState_];
    }

    TTabletBase::SetOwner(owner);
}

void TTablet::CopyFrom(const TTabletBase& otherBase)
{
    TTabletBase::CopyFrom(otherBase);

    YT_VERIFY(otherBase.GetType() == EObjectType::Tablet);
    auto* other = otherBase.As<TTablet>();

    PivotKey_ = other->PivotKey_;
    TrimmedRowCount_ = other->TrimmedRowCount_;
    ConflictHorizonTimestamp_ = other->ConflictHorizonTimestamp_;
    EdenStoreIds_ = other->EdenStoreIds_;
    BackupCutoffDescriptor_ = other->BackupCutoffDescriptor_;
    ReplicationProgress_ = other->ReplicationProgress_;
}

TTableReplicaInfo* TTablet::FindReplicaInfo(const TTableReplica* replica)
{
    auto it = Replicas_.find(const_cast<TTableReplica*>(replica));
    return it == Replicas_.end() ? nullptr : &it->second;
}

TTableReplicaInfo* TTablet::GetReplicaInfo(const TTableReplica* replica)
{
    auto* replicaInfo = FindReplicaInfo(replica);
    YT_VERIFY(replicaInfo);
    return replicaInfo;
}

TDuration TTablet::ComputeReplicationLagTime(
    TTimestamp latestTimestamp,
    const TTableReplicaInfo& replicaInfo,
    TTimestamp startReplicationTimestamp) const
{
    auto lastWriteTimestamp = FromProto<NTransactionClient::TTimestamp>(NodeStatistics_.last_write_timestamp());
    if (lastWriteTimestamp == NullTimestamp) {
        return TDuration::Zero();
    }

    auto replicationTimestamp = replicaInfo.GetCurrentReplicationTimestamp();
    // NB: Newly initialized replica with yet no rows in it shall display zero lag.
    if (replicationTimestamp == NullTimestamp &&
        (startReplicationTimestamp >= lastWriteTimestamp ||
         replicaInfo.GetCommittedReplicationRowIndex() >= NodeStatistics_.total_row_count()))
    {
        return TDuration::Zero();
    }

    if (replicationTimestamp >= lastWriteTimestamp || replicationTimestamp >= latestTimestamp) {
        return TDuration::Zero();
    }

    return TimestampToInstant(latestTimestamp).second - TimestampToInstant(replicationTimestamp).first;
}

TTabletStatistics TTablet::GetTabletStatistics(bool fromAuxiliaryCell) const
{
    const auto* table = GetTable();

    TTabletStatistics tabletStatistics;

    const auto& nodeStatistics = fromAuxiliaryCell
        ? AuxiliaryNodeStatistics_
        : NodeStatistics_;

    tabletStatistics.PartitionCount = nodeStatistics.partition_count();
    tabletStatistics.StoreCount = nodeStatistics.store_count();
    tabletStatistics.PreloadPendingStoreCount = nodeStatistics.preload_pending_store_count();
    tabletStatistics.PreloadCompletedStoreCount = nodeStatistics.preload_completed_store_count();
    tabletStatistics.PreloadFailedStoreCount = nodeStatistics.preload_failed_store_count();
    tabletStatistics.OverlappingStoreCount = nodeStatistics.overlapping_store_count();
    tabletStatistics.DynamicMemoryPoolSize = nodeStatistics.dynamic_memory_pool_size();
    // TODO(akozhikhov): Get rid of HunkCompressedDataSize?
    tabletStatistics.HunkUncompressedDataSize = GetHunkDataSize();
    tabletStatistics.HunkCompressedDataSize = GetHunkDataSize();
    tabletStatistics.MemorySize = GetTabletStaticMemorySize();
    tabletStatistics.TabletCount = 1;
    tabletStatistics.TabletCountPerMemoryMode[GetInMemoryMode()] = 1;

    auto& chunkTreeStatistics = GetChunkList()->Statistics();

    tabletStatistics.UnmergedRowCount += chunkTreeStatistics.RowCount;
    tabletStatistics.UncompressedDataSize += chunkTreeStatistics.UncompressedDataSize;
    tabletStatistics.UncompressedDataSize += chunkTreeStatistics.HunkDataSize;
    tabletStatistics.CompressedDataSize += chunkTreeStatistics.CompressedDataSize;
    tabletStatistics.CompressedDataSize += chunkTreeStatistics.HunkDataSize;
    tabletStatistics.ChunkCount += chunkTreeStatistics.ChunkCount;

    auto accumulateDiskSpaceStatistics = [&] (const auto& replication, auto regularDiskSpace, auto erasureDiskSpace) {
        for (const auto& entry : replication) {
            tabletStatistics.DiskSpacePerMedium[entry.GetMediumIndex()] += CalculateDiskSpaceUsage(
                entry.Policy().GetReplicationFactor(),
                regularDiskSpace,
                erasureDiskSpace);
        }
    };

    accumulateDiskSpaceStatistics(
        table->Replication(),
        chunkTreeStatistics.RegularDiskSpace,
        chunkTreeStatistics.ErasureDiskSpace);

    if (chunkTreeStatistics.HunkRegularDiskSpace != 0 || chunkTreeStatistics.HunkErasureDiskSpace > 0) {
        if (table->HunkReplication().GetSize() == 0) {
            accumulateDiskSpaceStatistics(
                table->Replication(),
                chunkTreeStatistics.HunkRegularDiskSpace,
                chunkTreeStatistics.HunkErasureDiskSpace);
        } else {
            accumulateDiskSpaceStatistics(
                table->HunkReplication(),
                chunkTreeStatistics.HunkRegularDiskSpace,
                chunkTreeStatistics.HunkErasureDiskSpace);
        }
    }

    // NB: Hunk chunks are not unique to a tablet so when accumulating over tablets this field will account duplicates.
    auto& hunkChunkTreeStatistics = GetHunkChunkList()->HunkStatistics();
    tabletStatistics.ChunkCount += hunkChunkTreeStatistics.ChunkCount;

    return tabletStatistics;
}

i64 TTablet::GetTabletMasterMemoryUsage() const
{
    return sizeof(TTablet) + GetDataWeight(GetPivotKey()) + EdenStoreIds_.size() * sizeof(TStoreId);
}

i64 TTablet::GetHunkDataWeight() const
{
    return GetChunkList()->Statistics().HunkDataWeight;
}

i64 TTablet::GetHunkDataSize() const
{
    return GetChunkList()->Statistics().HunkDataSize;
}

ETabletBackupState TTablet::GetBackupState() const
{
    return BackupState_;
}

void TTablet::SetBackupState(ETabletBackupState state)
{
    if (auto* table = GetTable()) {
        auto* trunkTable = table->GetTrunkNode();
        YT_VERIFY(trunkTable->TabletCountByBackupState()[BackupState_] > 0);
        --trunkTable->MutableTabletCountByBackupState()[BackupState_];
        ++trunkTable->MutableTabletCountByBackupState()[state];
    }

    BackupState_ = state;
}

void TTablet::CheckedSetBackupState(ETabletBackupState previous, ETabletBackupState next)
{
    YT_VERIFY(BackupState_ == previous);
    SetBackupState(next);
}

void TTablet::ValidateUnmount()
{
    TTabletBase::ValidateUnmount();

    for (auto it : GetIteratorsSortedByKey(Replicas())) {
        const auto& [weakReplica, replicaInfo] = *it;
        if (!IsObjectAlive(weakReplica)) {
            YT_TLOG_ALERT("Found zombie replica during tablet unmount validation")
                .With("TabletId", GetId())
                .With("ReplicaId", weakReplica->GetId());
            continue;
        }
        auto* replica = weakReplica.Get();
        if (replica->TransitioningTablets().contains(this)) {
            THROW_ERROR_EXCEPTION("Cannot unmount tablet %v since replica %v is in %Qlv state",
                Id_,
                replica->GetId(),
                replicaInfo.GetState());
        }
    }
}

void TTablet::ValidateReshardRemove() const
{
    TTabletBase::ValidateReshardRemove();

    // For ordered tablets, if the number of tablets decreases then validate that the trailing ones
    // (which we are about to drop) are properly trimmed.
    const auto& chunkListStatistics = GetChunkList()->Statistics();
    if (GetTrimmedRowCount() != chunkListStatistics.LogicalRowCount - chunkListStatistics.RowCount) {
        THROW_ERROR_EXCEPTION("Some chunks of tablet %v are not fully trimmed; such a tablet cannot "
            "participate in resharding",
            Id_);
    }
}

TOwningKeyBound TTablet::GetPivotKeyBound() const
{
    return TOwningKeyBound::FromRow() >= PivotKey_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
