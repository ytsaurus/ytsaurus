#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/object.h>

#include <yt/server/table_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/server/tablet_node/public.h>

#include <yt/server/chunk_server/public.h>

#include <yt/ytlib/tablet_client/heartbeat.pb.h>

#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellStatistics
{
    i64 UnmergedRowCount = 0;
    i64 UncompressedDataSize = 0;
    i64 CompressedDataSize = 0;
    i64 MemorySize = 0;
    i64 DynamicMemoryPoolSize = 0;
    i64 DiskSpacePerMedium[NChunkClient::MaxMediumCount] = {};
    int ChunkCount = 0;
    int PartitionCount = 0;
    int StoreCount = 0;
    int PreloadPendingStoreCount = 0;
    int PreloadCompletedStoreCount = 0;
    int PreloadFailedStoreCount = 0;
    TEnumIndexedVector<int, NTabletClient::EInMemoryMode> TabletCountPerMemoryMode;

    void Persist(NCellMaster::TPersistenceContext& context);
};

struct TTabletStatisticsBase
{
    int OverlappingStoreCount = 0;

    void Persist(NCellMaster::TPersistenceContext& context);
};

struct TTabletStatistics
    : public TTabletCellStatistics
    , public TTabletStatisticsBase
{
    void Persist(NCellMaster::TPersistenceContext& context);
};

TTabletCellStatistics& operator += (TTabletCellStatistics& lhs, const TTabletCellStatistics& rhs);
TTabletCellStatistics  operator +  (const TTabletCellStatistics& lhs, const TTabletCellStatistics& rhs);

TTabletStatistics& operator += (TTabletStatistics& lhs, const TTabletStatistics& rhs);
TTabletStatistics  operator +  (const TTabletStatistics& lhs, const TTabletStatistics& rhs);

TTabletCellStatistics& operator -= (TTabletCellStatistics& lhs, const TTabletCellStatistics& rhs);
TTabletCellStatistics  operator -  (const TTabletCellStatistics& lhs, const TTabletCellStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

class TSerializableTabletCellStatistics
    : public virtual NYTree::TYsonSerializable
    , public TTabletCellStatistics
{
public:
    TSerializableTabletCellStatistics();

    TSerializableTabletCellStatistics(
        const TTabletCellStatistics& statistics,
        const NChunkServer::TChunkManagerPtr& chunkManager);

private:
    int TabletCount_ = 0;
    i64 DiskSpace_ = 0;
    THashMap<TString, i64> DiskSpacePerMediumMap_;

    void InitParameters();
};

class TSerializableTabletStatisticsBase
    : public virtual NYTree::TYsonSerializable
    , public TTabletStatisticsBase
{
public:
    TSerializableTabletStatisticsBase();

    explicit TSerializableTabletStatisticsBase(const TTabletStatisticsBase& statistics);

private:
    void InitParameters();
};

class TSerializableTabletStatistics
    : public TSerializableTabletCellStatistics
    , public TSerializableTabletStatisticsBase
{
public:
    TSerializableTabletStatistics();

    TSerializableTabletStatistics(
        const TTabletStatistics& statistics,
        const NChunkServer::TChunkManagerPtr& chunkManager);
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletPerformanceCounter
{
    i64 Count = 0;
    double Rate = 0.0;
};

#define ITERATE_TABLET_PERFORMANCE_COUNTERS(XX) \
    XX(dynamic_row_read,                        DynamicRowRead) \
    XX(dynamic_row_lookup,                      DynamicRowLookup) \
    XX(dynamic_row_write,                       DynamicRowWrite) \
    XX(dynamic_row_write_data_weight,           DynamicRowWriteDataWeightCount) \
    XX(dynamic_row_delete,                      DynamicRowDelete) \
    XX(static_chunk_row_read,                   StaticChunkRowRead) \
    XX(static_chunk_row_lookup,                 StaticChunkRowLookup) \
    XX(static_chunk_row_lookup_true_negative,   StaticChunkRowLookupTrueNegative) \
    XX(static_chunk_row_lookup_false_positive,  StaticChunkRowLookupFalsePositive) \
    XX(unmerged_row_read,                       UnmergedRowRead) \
    XX(merged_row_read,                         MergedRowRead)

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
    DEFINE_BYVAL_RW_PROPERTY(i64, CurrentReplicationRowIndex, 0);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, CurrentReplicationTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

public:
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TTablet>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, Index);
    DEFINE_BYVAL_RW_PROPERTY(i64, MountRevision);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, StoresUpdatePreparedTransaction);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCell*, Cell);
    DEFINE_BYVAL_RW_PROPERTY(TTabletAction*, Action);
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TOwningKey, PivotKey);
    DEFINE_BYREF_RW_PROPERTY(NTabletClient::NProto::TTabletStatistics, NodeStatistics);
    DEFINE_BYREF_RW_PROPERTY(TTabletPerformanceCounters, PerformanceCounters);
    //! Only makes sense for mounted tablets.
    DEFINE_BYVAL_RW_PROPERTY(NTabletClient::EInMemoryMode, InMemoryMode);
    //! Only used for ordered tablets.
    DEFINE_BYVAL_RW_PROPERTY(i64, TrimmedRowCount);

    using TReplicaMap = THashMap<TTableReplica*, TTableReplicaInfo>;
    DEFINE_BYREF_RW_PROPERTY(TReplicaMap, Replicas);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetainedTimestamp);

public:
    explicit TTablet(const TTabletId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void CopyFrom(const TTablet& other);

    void ValidateMountRevision(i64 mountRevision);

    TTableReplicaInfo* FindReplicaInfo(const TTableReplica* replica);
    TTableReplicaInfo* GetReplicaInfo(const TTableReplica* replica);
    TDuration ComputeReplicationLagTime(const TTableReplicaInfo& replicaInfo) const;

    bool IsActive() const;

    NChunkServer::TChunkList* GetChunkList();
    const NChunkServer::TChunkList* GetChunkList() const;

    i64 GetTabletStaticMemorySize(NTabletClient::EInMemoryMode mode) const;
    i64 GetTabletStaticMemorySize() const;

    ETabletState GetState() const;
    void SetState(ETabletState state);

    NTableServer::TTableNode* GetTable() const;
    void SetTable(NTableServer::TTableNode* table);

private:
    ETabletState State_ = ETabletState::Unmounted;
    NTableServer::TTableNode* Table_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
