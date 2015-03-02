#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/enum.h>
#include <core/misc/ref_tracked.h>
#include <core/misc/nullable.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/object_server/object.h>

#include <server/table_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletStatistics
{
    TNullable<int> PartitionCount = 0;
    TNullable<int> StoreCount = 0;
    i64 UnmergedRowCount = 0;
    i64 UncompressedDataSize = 0;
    i64 CompressedDataSize = 0;
    i64 DiskSpace = 0;
    int ChunkCount = 0;
};

void Serialize(const TTabletStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TTabletPerformanceCounter
{
    i64 Count = 0;
    double Rate = 0.0;
};

#define ITERATE_TABLET_PERFORMANCE_COUNTERS(XX) \
    XX(dynamic_memory_row_read,     DynamicMemoryRowRead) \
    XX(dynamic_memory_row_lookup,   DynamicMemoryRowLookup) \
    XX(dynamic_memory_row_write,    DynamicMemoryRowWrite) \
    XX(dynamic_memory_row_delete,   DynamicMemoryRowDelete) \
    XX(static_chunk_row_read,       StaticChunkRowRead) \
    XX(static_chunk_row_lookup,     StaticChunkRowLookup) \
    XX(unmerged_row_read,           UnmergedRowRead) \
    XX(merged_row_read,             MergedRowRead)

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
    DEFINE_BYVAL_RW_PROPERTY(NTableServer::TTableNode*, Table);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCell*, Cell);
    DEFINE_BYVAL_RW_PROPERTY(NVersionedTableClient::TOwningKey, PivotKey);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TTabletStatistics, NodeStatistics);
    DEFINE_BYREF_RW_PROPERTY(TTabletPerformanceCounters, PerformanceCounters);

public:
    explicit TTablet(const TTabletId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
