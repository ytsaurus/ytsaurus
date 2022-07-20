#pragma once

#include "public.h"

#include "tablet_base.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_server/proto/backup_manager.pb.h>

#include <yt/yt/ytlib/tablet_client/backup.h>

#include <yt/yt/ytlib/tablet_client/proto/heartbeat.pb.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/aggregate_property.h>
#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletPerformanceCounter
{
    i64 Count = 0;
    double Rate = 0.0;
    double Rate10 = 0.0;
    double Rate60 = 0.0;
};

#define ITERATE_TABLET_PERFORMANCE_COUNTERS(XX) \
    XX(dynamic_row_read,                        DynamicRowRead) \
    XX(dynamic_row_read_data_weight,            DynamicRowReadDataWeight) \
    XX(dynamic_row_lookup,                      DynamicRowLookup) \
    XX(dynamic_row_lookup_data_weight,          DynamicRowLookupDataWeight) \
    XX(dynamic_row_write,                       DynamicRowWrite) \
    XX(dynamic_row_write_data_weight,           DynamicRowWriteDataWeight) \
    XX(dynamic_row_delete,                      DynamicRowDelete) \
    XX(static_chunk_row_read,                   StaticChunkRowRead) \
    XX(static_chunk_row_read_data_weight,       StaticChunkRowReadDataWeight) \
    XX(static_chunk_row_lookup,                 StaticChunkRowLookup) \
    XX(static_chunk_row_lookup_true_negative,   StaticChunkRowLookupTrueNegative) \
    XX(static_chunk_row_lookup_false_positive,  StaticChunkRowLookupFalsePositive) \
    XX(static_chunk_row_lookup_data_weight,     StaticChunkRowLookupDataWeight) \
    XX(unmerged_row_read,                       UnmergedRowRead) \
    XX(merged_row_read,                         MergedRowRead) \
    XX(compaction_data_weight,                  CompactionDataWeight) \
    XX(partitioning_data_weight,                PartitioningDataWeight) \
    XX(lookup_error,                            LookupErrorCount) \
    XX(write_error,                             WriteErrorCount)

struct TTabletPerformanceCounters
{
    TInstant Timestamp;
    #define XX(name, Name) TTabletPerformanceCounter Name;
    ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX
};

void Serialize(const TTabletPerformanceCounters& counters, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TTableReplicaInfo
{
public:
    DEFINE_BYVAL_RW_PROPERTY(ETableReplicaState, State, ETableReplicaState::None);
    DEFINE_BYVAL_RW_PROPERTY(i64, CommittedReplicationRowIndex, 0);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, CurrentReplicationTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(bool, HasError);

public:
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Populate(NTabletClient::NProto::TTableReplicaStatistics* statistics) const;
    void MergeFrom(const NTabletClient::NProto::TTableReplicaStatistics& statistics);
};

////////////////////////////////////////////////////////////////////////////////

struct TBackupCutoffDescriptor
{
    // Ordered tables.
    i64 CutoffRowIndex = 0;
    TStoreId NextDynamicStoreId;

    // Sorted tables.
    THashSet<NTabletClient::TDynamicStoreId> DynamicStoreIdsToKeep;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

TString ToString(const TBackupCutoffDescriptor& descriptor);

void FromProto(
    TBackupCutoffDescriptor* descriptor,
    const NProto::TBackupCutoffDescriptor& protoDescriptor);

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public TTabletBase
    , public TRefTracked<TTablet>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TLegacyOwningKey, PivotKey);
    DEFINE_BYREF_RW_PROPERTY(NTabletClient::NProto::TTabletStatistics, NodeStatistics);
    DEFINE_BYREF_RW_PROPERTY(TTabletPerformanceCounters, PerformanceCounters);
    //! Only used for ordered tablets.
    DEFINE_BYVAL_RW_PROPERTY(i64, TrimmedRowCount);

    DEFINE_BYVAL_RW_PROPERTY(i64, ReplicationErrorCount);

    using TReplicaMap = THashMap<TTableReplica*, TTableReplicaInfo>;
    DEFINE_BYREF_RW_PROPERTY(TReplicaMap, Replicas);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetainedTimestamp);

    using TUnconfirmedDynamicTableLocksSet = THashSet<NTransactionClient::TTransactionId>;
    DEFINE_BYREF_RW_PROPERTY(TUnconfirmedDynamicTableLocksSet, UnconfirmedDynamicTableLocks);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TStoreId>, EdenStoreIds);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NChunkServer::TDynamicStore*>, DynamicStores);

    DECLARE_BYVAL_RW_PROPERTY(ETabletBackupState, BackupState);
    DEFINE_BYREF_RW_PROPERTY(std::optional<TBackupCutoffDescriptor>, BackupCutoffDescriptor);

    using TIdIndexedReplicaMap = THashMap<TTableReplicaId, TTableReplicaInfo>;
    DEFINE_BYREF_RW_PROPERTY(TIdIndexedReplicaMap, BackedUpReplicaInfos);

    DEFINE_BYREF_RW_PROPERTY(NChaosClient::TReplicationProgress, ReplicationProgress);

public:
    using TTabletBase::TTabletBase;

    explicit TTablet(TTabletId tablet);

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    NTableServer::TTableNode* GetTable() const;

    void SetOwner(TTabletOwnerBase* owner) override;

    void CopyFrom(const TTabletBase& other) override;

    TTableReplicaInfo* FindReplicaInfo(const TTableReplica* replica);
    TTableReplicaInfo* GetReplicaInfo(const TTableReplica* replica);
    TDuration ComputeReplicationLagTime(
        NTransactionClient::TTimestamp latestTimestamp,
        const TTableReplicaInfo& replicaInfo) const;

    TTabletStatistics GetTabletStatistics() const override;

    i64 GetTabletMasterMemoryUsage() const override;

    i64 GetHunkUncompressedDataSize() const;
    i64 GetHunkCompressedDataSize() const;

    void CheckedSetBackupState(ETabletBackupState previous, ETabletBackupState next);

    void ValidateUnmount() override;
    void ValidateReshardRemove() const override;

private:
    ETabletBackupState BackupState_ = ETabletBackupState::None;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
