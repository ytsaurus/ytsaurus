#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/object.h>

#include <yt/server/table_server/public.h>

#include <yt/server/tablet_node/public.h>

#include <yt/ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletStatistics
{
    i64 UnmergedRowCount = 0;
    i64 UncompressedDataSize = 0;
    i64 CompressedDataSize = 0;
    i64 MemorySize = 0;
    i64 DiskSpace = 0;
    int ChunkCount = 0;
    int PartitionCount = 0;
    int StoreCount = 0;
    int PreloadPendingStoreCount = 0;
    int PreloadCompletedStoreCount = 0;
    int PreloadFailedStoreCount = 0;

    void Persist(NCellMaster::TPersistenceContext& context);
};

TTabletStatistics& operator += (TTabletStatistics& lhs, const TTabletStatistics& rhs);
TTabletStatistics  operator +  (const TTabletStatistics& lhs, const TTabletStatistics& rhs);

TTabletStatistics& operator -= (TTabletStatistics& lhs, const TTabletStatistics& rhs);
TTabletStatistics  operator -  (const TTabletStatistics& lhs, const TTabletStatistics& rhs);

void Serialize(const TTabletStatistics& statistics, NYson::IYsonConsumer* consumer);

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

class TTablet
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TTablet>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, Index);
    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State);
    DEFINE_BYVAL_RW_PROPERTY(i64, MountRevision);
    DEFINE_BYVAL_RW_PROPERTY(NTableServer::TTableNode*, Table);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCell*, Cell);
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TOwningKey, PivotKey);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TTabletStatistics, NodeStatistics);
    DEFINE_BYREF_RW_PROPERTY(TTabletPerformanceCounters, PerformanceCounters);
    //! Only makes sense for mounted tablets.
    DEFINE_BYVAL_RW_PROPERTY(NTabletNode::EInMemoryMode, InMemoryMode);
    //! Only used for ordered tablets.
    DEFINE_BYVAL_RW_PROPERTY(i64, FlushedRowCount);
    //! Only used for ordered tablets. Kept in sync with trimmed row count at node.
    DEFINE_BYVAL_RW_PROPERTY(i64, TrimmedRowCount);
    //! Only used for ordered tablets. Only counts whole trimmed stores.
    DEFINE_BYVAL_RW_PROPERTY(i64, TrimmedStoresRowCount);

public:
    explicit TTablet(const TTabletId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void CopyFrom(const TTablet& other);

    void ValidateMountRevision(i64 mountRevision);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
