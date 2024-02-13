#include "tablet.h"

#include "tablet_cell.h"
#include "table_replica.h"
#include "tablet_action.h"

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_tree_traverser.h>
#include <yt/yt/server/master/chunk_server/medium_base.h>
#include <yt/yt/server/master/chunk_server/dynamic_store.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypressClient;
using namespace NTableServer;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TTableReplicaInfo::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, State_);
    Save(context, CommittedReplicationRowIndex_);
    Save(context, CurrentReplicationTimestamp_);
    Save(context, HasError_);
}

void TTableReplicaInfo::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, State_);
    Load(context, CommittedReplicationRowIndex_);
    Load(context, CurrentReplicationTimestamp_);
    Load(context, HasError_);
}

void TTableReplicaInfo::Populate(
    NTabletClient::NProto::TTableReplicaStatistics* statistics) const
{
    statistics->set_committed_replication_row_index(GetCommittedReplicationRowIndex());
    statistics->set_current_replication_timestamp(GetCurrentReplicationTimestamp());
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
        statistics.current_replication_timestamp()));
}

////////////////////////////////////////////////////////////////////////////////

void TBackupCutoffDescriptor::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, CutoffRowIndex);
    Persist(context, NextDynamicStoreId);
    Persist(context, DynamicStoreIdsToKeep);
}

TString ToString(const TBackupCutoffDescriptor& descriptor)
{
    return Format(
        "{%v/%v/%v}",
        descriptor.CutoffRowIndex,
        descriptor.NextDynamicStoreId,
        descriptor.DynamicStoreIdsToKeep);
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
    , RetainedTimestamp_(MinTimestamp)
{ }

TString TTablet::GetLowercaseObjectName() const
{
    return Format("tablet %v", GetId());
}

TString TTablet::GetCapitalizedObjectName() const
{
    return Format("Tablet %v", GetId());
}

TString TTablet::GetObjectPath() const
{
    return Format("//sys/tablets/%v", GetId());
}

void TTablet::Save(TSaveContext& context) const
{
    TTabletBase::Save(context);

    using NYT::Save;
    Save(context, PivotKey_);
    Save(context, NodeStatistics_);
    Save(context, TrimmedRowCount_);
    Save(context, Replicas_);
    Save(context, RetainedTimestamp_);
    Save(context, ReplicationErrorCount_);
    Save(context, UnconfirmedDynamicTableLocks_);
    Save(context, EdenStoreIds_);
    Save(context, BackupState_);
    Save(context, BackupCutoffDescriptor_);
    Save(context, BackedUpReplicaInfos_);
    Save(context, DynamicStores_);
    Save(context, ReplicationProgress_);
}

void TTablet::Load(TLoadContext& context)
{
    TTabletBase::Load(context);

    using NYT::Load;
    Load(context, PivotKey_);
    Load(context, NodeStatistics_);
    Load(context, TrimmedRowCount_);
    Load(context, Replicas_);
    Load(context, RetainedTimestamp_);
    Load(context, ReplicationErrorCount_);
    Load(context, UnconfirmedDynamicTableLocks_);
    Load(context, EdenStoreIds_);
    Load(context, BackupState_);
    Load(context, BackupCutoffDescriptor_);
    Load(context, BackedUpReplicaInfos_);
    Load(context, DynamicStores_);
    Load(context, ReplicationProgress_);
}

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

TDuration TTablet::ComputeReplicationLagTime(TTimestamp latestTimestamp, const TTableReplicaInfo& replicaInfo) const
{
    auto lastWriteTimestamp = NodeStatistics_.last_write_timestamp();
    if (lastWriteTimestamp == NullTimestamp) {
        return TDuration::Zero();
    }
    auto replicationTimestamp = replicaInfo.GetCurrentReplicationTimestamp();
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
    tabletStatistics.HunkUncompressedDataSize = GetHunkUncompressedDataSize();
    tabletStatistics.HunkCompressedDataSize = GetHunkCompressedDataSize();
    tabletStatistics.MemorySize = GetTabletStaticMemorySize();
    tabletStatistics.TabletCount = 1;
    tabletStatistics.TabletCountPerMemoryMode[GetInMemoryMode()] = 1;

    for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
        if (const auto* chunkList = GetChunkList(contentType)) {
            const auto& treeStatistics = chunkList->Statistics();

            tabletStatistics.UnmergedRowCount += treeStatistics.RowCount;
            tabletStatistics.UncompressedDataSize += treeStatistics.UncompressedDataSize;
            tabletStatistics.CompressedDataSize += treeStatistics.CompressedDataSize;

            for (const auto& entry : table->Replication()) {
                tabletStatistics.DiskSpacePerMedium[entry.GetMediumIndex()] += CalculateDiskSpaceUsage(
                    entry.Policy().GetReplicationFactor(),
                    treeStatistics.RegularDiskSpace,
                    treeStatistics.ErasureDiskSpace);
            }
            tabletStatistics.ChunkCount += treeStatistics.ChunkCount;
        }
    }

    return tabletStatistics;
}

i64 TTablet::GetTabletMasterMemoryUsage() const
{
    return sizeof(TTablet) + GetDataWeight(GetPivotKey()) + EdenStoreIds_.size() * sizeof(TStoreId);
}

i64 TTablet::GetHunkUncompressedDataSize() const
{
    return GetHunkChunkList()->Statistics().UncompressedDataSize;
}

i64 TTablet::GetHunkCompressedDataSize() const
{
    return GetHunkChunkList()->Statistics().CompressedDataSize;
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
        const auto* replica = it->first;
        const auto& replicaInfo = it->second;
        if (replica->TransitioningTablets().count(this) > 0) {
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
