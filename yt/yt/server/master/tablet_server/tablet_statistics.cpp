#include "tablet_statistics.h"

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

namespace NYT::NTabletServer {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TTabletCellStatisticsBase::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, UnmergedRowCount);
    Persist(context, UncompressedDataSize);
    Persist(context, CompressedDataSize);
    Persist(context, HunkUncompressedDataSize);
    Persist(context, HunkCompressedDataSize);
    Persist(context, MemorySize);
    Persist(context, DiskSpacePerMedium);
    Persist(context, ChunkCount);
    Persist(context, PartitionCount);
    Persist(context, StoreCount);
    Persist(context, PreloadPendingStoreCount);
    Persist(context, PreloadCompletedStoreCount);
    Persist(context, PreloadFailedStoreCount);
    Persist(context, TabletCount);
    Persist(context, TabletCountPerMemoryMode);
    Persist(context, DynamicMemoryPoolSize);
}

////////////////////////////////////////////////////////////////////////////////

void TTabletCellStatistics::Persist(const NCellMaster::TPersistenceContext& context)
{
    TTabletCellStatisticsBase::Persist(context);
}

////////////////////////////////////////////////////////////////////////////////

void TTabletStatisticsBase::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, OverlappingStoreCount);
}

////////////////////////////////////////////////////////////////////////////////

void TTabletStatistics::Persist(const NCellMaster::TPersistenceContext& context)
{
    TTabletCellStatisticsBase::Persist(context);
    TTabletStatisticsBase::Persist(context);
}

////////////////////////////////////////////////////////////////////////////////

TTabletStatistics TTabletStatisticsAggregate::Get() const
{
    auto statistics = CellStatistics_.Get();
    statistics.OverlappingStoreCount = OverlappingStoreCount_.Get();
    return statistics;
}

void TTabletStatisticsAggregate::Account(const TTabletStatistics& tabletStatistics)
{
    CellStatistics_.Account(tabletStatistics);
    OverlappingStoreCount_.Account(tabletStatistics.OverlappingStoreCount);
}

void TTabletStatisticsAggregate::Discount(const TTabletStatistics& tabletStatistics)
{
    CellStatistics_.Discount(tabletStatistics);
    OverlappingStoreCount_.Discount(tabletStatistics.OverlappingStoreCount);
}

void TTabletStatisticsAggregate::AccountDelta(const TTabletStatistics& tabletStatistics)
{
    CellStatistics_.AccountDelta(tabletStatistics);

    YT_VERIFY(tabletStatistics.OverlappingStoreCount == 0);
}

void TTabletStatisticsAggregate::Reset()
{
    CellStatistics_.Reset();
    OverlappingStoreCount_.Reset();
}

void TTabletStatisticsAggregate::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, CellStatistics_);
    Save(context, OverlappingStoreCount_);
}

void TTabletStatisticsAggregate::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, CellStatistics_);
    Load(context, OverlappingStoreCount_);
}

////////////////////////////////////////////////////////////////////////////////

TTabletCellStatisticsBase& operator += (TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs)
{
    lhs.UnmergedRowCount += rhs.UnmergedRowCount;
    lhs.UncompressedDataSize += rhs.UncompressedDataSize;
    lhs.CompressedDataSize += rhs.CompressedDataSize;
    lhs.HunkUncompressedDataSize += rhs.HunkUncompressedDataSize;
    lhs.HunkCompressedDataSize += rhs.HunkCompressedDataSize;
    lhs.MemorySize += rhs.MemorySize;
    for (const auto& [mediumIndex, diskSpace] : rhs.DiskSpacePerMedium) {
        lhs.DiskSpacePerMedium[mediumIndex] += diskSpace;
    }
    lhs.ChunkCount += rhs.ChunkCount;
    lhs.PartitionCount += rhs.PartitionCount;
    lhs.StoreCount += rhs.StoreCount;
    lhs.PreloadPendingStoreCount += rhs.PreloadPendingStoreCount;
    lhs.PreloadCompletedStoreCount += rhs.PreloadCompletedStoreCount;
    lhs.PreloadFailedStoreCount += rhs.PreloadFailedStoreCount;
    lhs.DynamicMemoryPoolSize += rhs.DynamicMemoryPoolSize;
    lhs.TabletCount += rhs.TabletCount;
    std::transform(
        std::begin(lhs.TabletCountPerMemoryMode),
        std::end(lhs.TabletCountPerMemoryMode),
        std::begin(rhs.TabletCountPerMemoryMode),
        std::begin(lhs.TabletCountPerMemoryMode),
        std::plus<i64>());
    return lhs;
}

TTabletCellStatisticsBase operator + (const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TTabletCellStatisticsBase& operator -= (TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs)
{
    lhs.UnmergedRowCount -= rhs.UnmergedRowCount;
    lhs.UncompressedDataSize -= rhs.UncompressedDataSize;
    lhs.CompressedDataSize -= rhs.CompressedDataSize;
    lhs.HunkUncompressedDataSize -= rhs.HunkUncompressedDataSize;
    lhs.HunkCompressedDataSize -= rhs.HunkCompressedDataSize;
    lhs.MemorySize -= rhs.MemorySize;
    for (const auto& [mediumIndex, diskSpace] : rhs.DiskSpacePerMedium) {
        lhs.DiskSpacePerMedium[mediumIndex] -= diskSpace;
    }
    lhs.ChunkCount -= rhs.ChunkCount;
    lhs.PartitionCount -= rhs.PartitionCount;
    lhs.StoreCount -= rhs.StoreCount;
    lhs.PreloadPendingStoreCount -= rhs.PreloadPendingStoreCount;
    lhs.PreloadCompletedStoreCount -= rhs.PreloadCompletedStoreCount;
    lhs.PreloadFailedStoreCount -= rhs.PreloadFailedStoreCount;
    lhs.DynamicMemoryPoolSize -= rhs.DynamicMemoryPoolSize;
    lhs.TabletCount -= rhs.TabletCount;
    std::transform(
        std::begin(lhs.TabletCountPerMemoryMode),
        std::end(lhs.TabletCountPerMemoryMode),
        std::begin(rhs.TabletCountPerMemoryMode),
        std::begin(lhs.TabletCountPerMemoryMode),
        std::minus<i64>());
    return lhs;
}

TTabletCellStatisticsBase operator - (const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

bool operator == (const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs)
{
    return
        lhs.UnmergedRowCount == rhs.UnmergedRowCount &&
        lhs.UncompressedDataSize == rhs.UncompressedDataSize &&
        lhs.CompressedDataSize == rhs.CompressedDataSize &&
        lhs.HunkUncompressedDataSize == rhs.HunkUncompressedDataSize &&
        lhs.HunkCompressedDataSize == rhs.HunkCompressedDataSize &&
        lhs.MemorySize == rhs.MemorySize &&
        lhs.DynamicMemoryPoolSize == rhs.DynamicMemoryPoolSize &&
        lhs.ChunkCount == rhs.ChunkCount &&
        lhs.PartitionCount == rhs.PartitionCount &&
        lhs.StoreCount == rhs.StoreCount &&
        lhs.PreloadPendingStoreCount == rhs.PreloadPendingStoreCount &&
        lhs.PreloadCompletedStoreCount == rhs.PreloadCompletedStoreCount &&
        lhs.PreloadFailedStoreCount == rhs.PreloadFailedStoreCount &&
        lhs.TabletCount == rhs.TabletCount &&
        std::equal(
            lhs.TabletCountPerMemoryMode.begin(),
            lhs.TabletCountPerMemoryMode.end(),
            rhs.TabletCountPerMemoryMode.begin()) &&
        lhs.DiskSpacePerMedium.size() == rhs.DiskSpacePerMedium.size() &&
        std::all_of(
            lhs.DiskSpacePerMedium.begin(),
            lhs.DiskSpacePerMedium.end(),
            [&] (const auto& value) {
                auto it = rhs.DiskSpacePerMedium.find(value.first);
                return it != rhs.DiskSpacePerMedium.end() && it->second == value.second;
            });
}

bool operator != (const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs)
{
    return !(lhs == rhs);
}

TTabletStatistics& operator += (TTabletStatistics& lhs, const TTabletStatistics& rhs)
{
    static_cast<TTabletCellStatisticsBase&>(lhs) += rhs;

    lhs.OverlappingStoreCount = std::max(lhs.OverlappingStoreCount, rhs.OverlappingStoreCount);
    return lhs;
}

TTabletStatistics operator + (const TTabletStatistics& lhs, const TTabletStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TTabletStatistics& operator -= (TTabletStatistics& lhs, const TTabletStatistics& rhs)
{
    static_cast<TTabletCellStatisticsBase&>(lhs) -= rhs;

    // Overlapping store count cannot be subtracted.

    return lhs;
}

TTabletStatistics operator - (const TTabletStatistics& lhs, const TTabletStatistics& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

bool operator == (const TTabletStatistics& lhs, const TTabletStatistics& rhs)
{
    return static_cast<const TTabletCellStatisticsBase&>(lhs) == static_cast<const TTabletCellStatisticsBase&>(rhs) &&
        lhs.OverlappingStoreCount == rhs.OverlappingStoreCount;
}

bool operator != (const TTabletStatistics& lhs, const TTabletStatistics& rhs)
{
    return !(lhs == rhs);
}

void ToProto(NProto::TTabletCellStatistics* protoStatistics, const TTabletCellStatistics& statistics)
{
    protoStatistics->set_unmerged_row_count(statistics.UnmergedRowCount);
    protoStatistics->set_uncompressed_data_size(statistics.UncompressedDataSize);
    protoStatistics->set_compressed_data_size(statistics.CompressedDataSize);
    protoStatistics->set_hunk_uncompressed_data_size(statistics.HunkUncompressedDataSize);
    protoStatistics->set_hunk_compressed_data_size(statistics.HunkCompressedDataSize);
    protoStatistics->set_memory_size(statistics.MemorySize);
    protoStatistics->set_chunk_count(statistics.ChunkCount);
    protoStatistics->set_partition_count(statistics.PartitionCount);
    protoStatistics->set_store_count(statistics.StoreCount);
    protoStatistics->set_preload_pending_store_count(statistics.PreloadPendingStoreCount);
    protoStatistics->set_preload_completed_store_count(statistics.PreloadCompletedStoreCount);
    protoStatistics->set_preload_failed_store_count(statistics.PreloadFailedStoreCount);
    protoStatistics->set_dynamic_memory_pool_size(statistics.DynamicMemoryPoolSize);
    protoStatistics->set_tablet_count(statistics.TabletCount);

    for (const auto& [mediumIndex, diskSpace] : statistics.DiskSpacePerMedium) {
        auto* item = protoStatistics->add_disk_space_per_medium();
        item->set_medium_index(mediumIndex);
        item->set_disk_space(diskSpace);
    }

    ToProto(protoStatistics->mutable_tablet_count_per_memory_mode(), statistics.TabletCountPerMemoryMode);
}

void FromProto(TTabletCellStatistics* statistics, const NProto::TTabletCellStatistics& protoStatistics)
{
    statistics->UnmergedRowCount = protoStatistics.unmerged_row_count();
    statistics->UncompressedDataSize = protoStatistics.uncompressed_data_size();
    statistics->CompressedDataSize = protoStatistics.compressed_data_size();
    statistics->HunkUncompressedDataSize = protoStatistics.hunk_uncompressed_data_size();
    statistics->HunkCompressedDataSize = protoStatistics.hunk_compressed_data_size();
    statistics->MemorySize = protoStatistics.memory_size();
    statistics->ChunkCount = protoStatistics.chunk_count();
    statistics->PartitionCount = protoStatistics.partition_count();
    statistics->StoreCount = protoStatistics.store_count();
    statistics->PreloadPendingStoreCount = protoStatistics.preload_pending_store_count();
    statistics->PreloadCompletedStoreCount = protoStatistics.preload_completed_store_count();
    statistics->PreloadFailedStoreCount = protoStatistics.preload_failed_store_count();
    statistics->DynamicMemoryPoolSize = protoStatistics.dynamic_memory_pool_size();
    statistics->TabletCount = protoStatistics.tablet_count();
    for (const auto& item : protoStatistics.disk_space_per_medium()) {
        statistics->DiskSpacePerMedium[item.medium_index()] = item.disk_space();
    }
    FromProto(&statistics->TabletCountPerMemoryMode, protoStatistics.tablet_count_per_memory_mode());
}

TString ToString(const TTabletStatistics& tabletStatistics, const IChunkManagerPtr& chunkManager)
{
    TStringStream output;
    TYsonWriter writer(&output, EYsonFormat::Text);
    New<TSerializableTabletStatistics>(tabletStatistics, chunkManager)->Save(&writer);
    writer.Flush();
    return output.Str();
}

////////////////////////////////////////////////////////////////////////////////

TSerializableTabletCellStatisticsBase::TSerializableTabletCellStatisticsBase(
    const TTabletCellStatisticsBase& statistics,
    const IChunkManagerPtr& chunkManager)
    : TTabletCellStatisticsBase(statistics)
{
    NYTree::TYsonStructRegistry::Get()->InitializeStruct(this);
    DiskSpace_ = 0;
    for (const auto& [mediumIndex, mediumDiskSpace] : DiskSpacePerMedium) {
        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        YT_VERIFY(DiskSpacePerMediumMap_.emplace(medium->GetName(), mediumDiskSpace).second);
        DiskSpace_ += mediumDiskSpace;
    }
}

void TSerializableTabletCellStatisticsBase::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("unmerged_row_count", &TThis::UnmergedRowCount);
    registrar.BaseClassParameter("uncompressed_data_size", &TThis::UncompressedDataSize);
    registrar.BaseClassParameter("compressed_data_size", &TThis::CompressedDataSize);
    registrar.BaseClassParameter("hunk_uncompressed_data_size", &TThis::HunkUncompressedDataSize);
    registrar.BaseClassParameter("hunk_compressed_data_size", &TThis::HunkCompressedDataSize);
    registrar.BaseClassParameter("memory_size", &TThis::MemorySize);
    registrar.Parameter("disk_space", &TThis::DiskSpace_);
    registrar.Parameter("disk_space_per_medium", &TThis::DiskSpacePerMediumMap_);
    registrar.BaseClassParameter("chunk_count", &TThis::ChunkCount);
    registrar.BaseClassParameter("partition_count", &TThis::PartitionCount);
    registrar.BaseClassParameter("store_count", &TThis::StoreCount);
    registrar.BaseClassParameter("preload_pending_store_count", &TThis::PreloadPendingStoreCount);
    registrar.BaseClassParameter("preload_completed_store_count", &TThis::PreloadCompletedStoreCount);
    registrar.BaseClassParameter("preload_failed_store_count", &TThis::PreloadFailedStoreCount);
    registrar.BaseClassParameter("dynamic_memory_pool_size", &TThis::DynamicMemoryPoolSize);
    registrar.BaseClassParameter("tablet_count", &TThis::TabletCount);
    registrar.BaseClassParameter("tablet_count_per_memory_mode", &TThis::TabletCountPerMemoryMode);
}

TSerializableTabletStatisticsBase::TSerializableTabletStatisticsBase(
    const TTabletStatisticsBase& statistics)
    : TTabletStatisticsBase(statistics)
{
    NYTree::TYsonStructRegistry::Get()->InitializeStruct(this);
}

void TSerializableTabletStatisticsBase::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("overlapping_store_count", &TThis::OverlappingStoreCount);
}

////////////////////////////////////////////////////////////////////////////////

TSerializableTabletCellStatistics::TSerializableTabletCellStatistics(
    const TTabletCellStatistics& statistics,
    const IChunkManagerPtr& chunkManager)
    : TSerializableTabletCellStatisticsBase(statistics, chunkManager)
{
    NYTree::TYsonStructRegistry::Get()->InitializeStruct(this);
}

TSerializableTabletStatistics::TSerializableTabletStatistics(
    const TTabletStatistics& statistics,
    const IChunkManagerPtr& chunkManager)
    : TSerializableTabletCellStatisticsBase(statistics, chunkManager)
    , TSerializableTabletStatisticsBase(statistics)
{
    NYTree::TYsonStructRegistry::Get()->InitializeStruct(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
