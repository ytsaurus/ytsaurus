#include "tablet.h"
#include "tablet_cell.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/server/table_server/table_node.h>

#include <yt/core/ytree/fluent.h>

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
    if (context.IsSave() || context.LoadContext().GetVersion() >= 122) {
        Persist(context, PreloadPendingStoreCount);
        Persist(context, PreloadCompletedStoreCount);
        Persist(context, PreloadFailedStoreCount);
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
    lhs.PreloadPendingStoreCount += rhs.PreloadPendingStoreCount;
    lhs.PreloadCompletedStoreCount += rhs.PreloadCompletedStoreCount;
    lhs.PreloadFailedStoreCount += rhs.PreloadFailedStoreCount;
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
    lhs.PreloadPendingStoreCount -= rhs.PreloadPendingStoreCount;
    lhs.PreloadCompletedStoreCount -= rhs.PreloadCompletedStoreCount;
    lhs.PreloadFailedStoreCount -= rhs.PreloadFailedStoreCount;
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
            .Item("preload_pending_store_count").Value(statistics.PreloadPendingStoreCount)
            .Item("preload_completed_store_count").Value(statistics.PreloadCompletedStoreCount)
            .Item("preload_failed_store_count").Value(statistics.PreloadFailedStoreCount)
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
    , InMemoryMode_(NTabletNode::EInMemoryMode::None)
{ }

void TTablet::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Index_);
    Save(context, State_);
    Save(context, MountRevision_);
    Save(context, Table_);
    Save(context, Cell_);
    Save(context, PivotKey_);
    Save(context, NodeStatistics_);
    Save(context, InMemoryMode_);
    Save(context, FlushedRowCount_);
    Save(context, TrimmedStoresRowCount_);
    Save(context, TrimmedRowCount_);
}

void TTablet::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Index_);
    Load(context, State_);
    Load(context, MountRevision_);
    Load(context, Table_);
    Load(context, Cell_);
    Load(context, PivotKey_);
    Load(context, NodeStatistics_);
    Load(context, InMemoryMode_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 401) {
        Load(context, FlushedRowCount_);
        Load(context, TrimmedStoresRowCount_);
        Load(context, TrimmedRowCount_);
    }
}

void TTablet::ValidateMountRevision(i64 mountRevision)
{
    if (MountRevision_ != mountRevision) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Invalid mount revision of tablet %v: expected %x, received %x",
            Id_,
            MountRevision_,
            mountRevision);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

