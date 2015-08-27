#include "stdafx.h"
#include "tablet.h"
#include "tablet_cell.h"

#include <core/ytree/fluent.h>

#include <server/table_server/table_node.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NTabletServer {

using namespace NTableServer;
using namespace NCellMaster;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TTabletStatistics::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, UnmergedRowCount);
    Persist(context, UncompressedDataSize);
    Persist(context, CompressedDataSize);
    Persist(context, MemorySize);
    Persist(context, DiskSpace);
    Persist(context, ChunkCount);
    Persist(context, PartitionCount);
    Persist(context, StoreCount);
    // COMPAT(sandello)
    if (context.IsSave() || context.LoadContext().GetVersion() >= 121) {
        Persist(context, StorePreloadPendingCount);
        Persist(context, StorePreloadCompletedCount);
        Persist(context, StorePreloadFailedCount);
    }
}

TTabletStatistics& operator +=(TTabletStatistics& lhs, const TTabletStatistics& rhs)
{
    lhs.UnmergedRowCount += rhs.UnmergedRowCount;
    lhs.UncompressedDataSize += rhs.UncompressedDataSize;
    lhs.CompressedDataSize += rhs.CompressedDataSize;
    lhs.MemorySize += rhs.MemorySize;
    lhs.DiskSpace += rhs.DiskSpace;
    lhs.ChunkCount += rhs.ChunkCount;
    lhs.PartitionCount += rhs.PartitionCount;
    lhs.StoreCount += rhs.StoreCount;
    lhs.StorePreloadPendingCount += rhs.StorePreloadPendingCount;
    lhs.StorePreloadCompletedCount += rhs.StorePreloadCompletedCount;
    lhs.StorePreloadFailedCount += rhs.StorePreloadFailedCount;
    return lhs;
}

TTabletStatistics operator +(const TTabletStatistics& lhs, const TTabletStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TTabletStatistics& operator -=(TTabletStatistics& lhs, const TTabletStatistics& rhs)
{
    lhs.UnmergedRowCount -= rhs.UnmergedRowCount;
    lhs.UncompressedDataSize -= rhs.UncompressedDataSize;
    lhs.CompressedDataSize -= rhs.CompressedDataSize;
    lhs.MemorySize -= rhs.MemorySize;
    lhs.DiskSpace -= rhs.DiskSpace;
    lhs.ChunkCount -= rhs.ChunkCount;
    lhs.PartitionCount -= rhs.PartitionCount;
    lhs.StoreCount -= rhs.StoreCount;
    lhs.StorePreloadPendingCount -= rhs.StorePreloadPendingCount;
    lhs.StorePreloadCompletedCount -= rhs.StorePreloadCompletedCount;
    lhs.StorePreloadFailedCount -= rhs.StorePreloadFailedCount;
    return lhs;
}

TTabletStatistics operator -(const TTabletStatistics& lhs, const TTabletStatistics& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

void Serialize(const TTabletStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("unmerged_row_count").Value(statistics.UnmergedRowCount)
            .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
            .Item("compressed_data_size").Value(statistics.CompressedDataSize)
            .Item("memory_size").Value(statistics.MemorySize)
            .Item("disk_space").Value(statistics.DiskSpace)
            .Item("chunk_count").Value(statistics.ChunkCount)
            .Item("partition_count").Value(statistics.PartitionCount)
            .Item("store_count").Value(statistics.StoreCount)
            .Item("store_preload_pending_count").Value(statistics.StorePreloadPendingCount)
            .Item("store_preload_completed_count").Value(statistics.StorePreloadCompletedCount)
            .Item("store_preload_failed_count").Value(statistics.StorePreloadFailedCount)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TTabletPerformanceCounters& counters, NYson::IYsonConsumer* consumer)
{
    #define XX(name, Name) \
        .Item(#name "_count").Value(counters.Name.Count) \
        .Item(#name "_rate").Value(counters.Name.Rate)
    BuildYsonFluently(consumer)
        .BeginMap()
            ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
        .EndMap();
    #undef XX
}

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(const TTabletId& id)
    : TNonversionedObjectBase(id)
    , Index_(-1)
    , State_(ETabletState::Unmounted)
    , Table_(nullptr)
    , Cell_(nullptr)
    , InMemoryMode_(NTabletNode::EInMemoryMode::None)
{ }

void TTablet::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Index_);
    Save(context, State_);
    Save(context, Table_);
    Save(context, Cell_);
    Save(context, PivotKey_);
    Save(context, NodeStatistics_);
    Save(context, InMemoryMode_);
}

void TTablet::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Index_);
    Load(context, State_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 115) {
        Load(context, Table_);
    } else {
        Load<i32>(context);
    }
    Load(context, Cell_);
    Load(context, PivotKey_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 111) {
        Load(context, NodeStatistics_);
    }
    // COMPAT(babenko)
    if (context.GetVersion() >= 119) {
        Load(context, InMemoryMode_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

