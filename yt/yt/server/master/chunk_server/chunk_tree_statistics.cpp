#include "chunk_tree_statistics.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TChunkTreeStatistics::Accumulate(const TChunkTreeStatistics& other)
{
    RowCount += other.RowCount;
    LogicalRowCount += other.LogicalRowCount;
    UncompressedDataSize += other.UncompressedDataSize;
    CompressedDataSize += other.CompressedDataSize;
    RegularDiskSpace += other.RegularDiskSpace;
    ErasureDiskSpace += other.ErasureDiskSpace;
    HunkDataWeight += other.HunkDataWeight;
    HunkDataSize += other.HunkDataSize;
    HunkRegularDiskSpace += other.HunkRegularDiskSpace;
    HunkErasureDiskSpace += other.HunkErasureDiskSpace;
    ChunkCount += other.ChunkCount;
    ChunkListCount += other.ChunkListCount;
    Rank = std::max(Rank, other.Rank);

    if (DataWeight == -1 || other.DataWeight == -1) {
        DataWeight = -1;
    } else {
        DataWeight += other.DataWeight;
    }
    if (LogicalDataWeight == -1 || other.LogicalDataWeight == -1) {
        LogicalDataWeight = -1;
    } else {
        LogicalDataWeight += other.LogicalDataWeight;
    }
}

void TChunkTreeStatistics::Deaccumulate(const TChunkTreeStatistics& other)
{
    RowCount -= other.RowCount;
    LogicalRowCount -= other.LogicalRowCount;
    UncompressedDataSize -= other.UncompressedDataSize;
    CompressedDataSize -= other.CompressedDataSize;
    RegularDiskSpace -= other.RegularDiskSpace;
    ErasureDiskSpace -= other.ErasureDiskSpace;
    HunkDataWeight -= other.HunkDataWeight;
    HunkDataSize -= other.HunkDataSize;
    HunkRegularDiskSpace -= other.HunkRegularDiskSpace;
    HunkErasureDiskSpace -= other.HunkErasureDiskSpace;
    ChunkCount -= other.ChunkCount;
    ChunkListCount -= other.ChunkListCount;
    // NB: Rank is ignored intentionally since there's no way to deaccumulate it.

    if (DataWeight == -1 || other.DataWeight == -1) {
        DataWeight = -1;
    } else {
        DataWeight -= other.DataWeight;
    }
    if (LogicalDataWeight == -1 || other.LogicalDataWeight == -1) {
        LogicalDataWeight = -1;
    } else {
        LogicalDataWeight -= other.LogicalDataWeight;
    }
}

TChunkOwnerDataStatistics TChunkTreeStatistics::ToDataStatistics() const
{
    TChunkOwnerDataStatistics result;
    result.UncompressedDataSize = UncompressedDataSize;
    result.CompressedDataSize = CompressedDataSize;
    result.DataWeight = DataWeight;
    result.RowCount = RowCount;
    result.ChunkCount = ChunkCount;
    result.RegularDiskSpace = RegularDiskSpace;
    result.ErasureDiskSpace = ErasureDiskSpace;

    result.DataWeight += HunkDataWeight;
    result.UncompressedDataSize += HunkDataSize;
    result.CompressedDataSize += HunkDataSize;

    // NB: HunkRegularDiskSpace and HunkErasureDiskSpace do not go anywhere
    // because they are irrelevant in the context of data statistics.

    return result;
}

void TChunkTreeStatistics::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, RowCount);
    Persist(context, LogicalRowCount);
    Persist(context, UncompressedDataSize);
    Persist(context, CompressedDataSize);
    Persist(context, DataWeight);
    Persist(context, LogicalDataWeight);
    Persist(context, RegularDiskSpace);
    Persist(context, ErasureDiskSpace);
    if ((context.GetVersion() >= NCellMaster::EMasterReign::HunkChunkTreeStatisticsOverhaul &&
         context.GetVersion() < NCellMaster::EMasterReign::Start_26_2) ||
        context.GetVersion() >= NCellMaster::EMasterReign::HunkChunkTreeStatisticsOverhaul_26_2)
    {
        Persist(context, HunkDataWeight);
        Persist(context, HunkDataSize);
        Persist(context, HunkRegularDiskSpace);
        Persist(context, HunkErasureDiskSpace);
    }
    Persist(context, ChunkCount);
    Persist(context, ChunkListCount);
    Persist(context, Rank);
}

bool TChunkTreeStatistics::operator==(const TChunkTreeStatistics& other) const
{
    return
        RowCount == other.RowCount &&
        LogicalRowCount == other.LogicalRowCount &&
        UncompressedDataSize == other.UncompressedDataSize &&
        CompressedDataSize == other.CompressedDataSize &&
        RegularDiskSpace == other.RegularDiskSpace &&
        ErasureDiskSpace == other.ErasureDiskSpace &&
        HunkDataWeight == other.HunkDataWeight &&
        HunkDataSize == other.HunkDataSize &&
        HunkRegularDiskSpace == other.HunkRegularDiskSpace &&
        HunkErasureDiskSpace == other.HunkErasureDiskSpace &&
        ChunkCount == other.ChunkCount &&
        ChunkListCount == other.ChunkListCount &&
        Rank == other.Rank &&
        (DataWeight == -1 || other.DataWeight == -1 || DataWeight == other.DataWeight) &&
        (LogicalDataWeight == -1 || other.LogicalDataWeight == -1 || LogicalDataWeight == other.LogicalDataWeight);
}

void FormatValue(TStringBuilderBase* builder, const TChunkTreeStatistics& statistics, TStringBuf spec)
{
    FormatValue(builder, ConvertToYsonString(statistics, EYsonFormat::Text), spec);
}

void Serialize(const TChunkTreeStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("row_count").Value(statistics.RowCount)
            .Item("logical_row_count").Value(statistics.LogicalRowCount)
            .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
            .Item("compressed_data_size").Value(statistics.CompressedDataSize)
            .Item("data_weight").Value(statistics.DataWeight)
            .Item("logical_data_weight").Value(statistics.LogicalDataWeight)
            .Item("trimmed_data_weight").Value(statistics.LogicalDataWeight - statistics.DataWeight)
            .Item("regular_disk_space").Value(statistics.RegularDiskSpace)
            .Item("erasure_disk_space").Value(statistics.ErasureDiskSpace)
            .Item("hunk_data_weight").Value(statistics.HunkDataWeight)
            .Item("hunk_data_size").Value(statistics.HunkDataSize)
            .Item("hunk_regular_disk_space").Value(statistics.HunkRegularDiskSpace)
            .Item("hunk_erasure_disk_space").Value(statistics.HunkErasureDiskSpace)
            .Item("chunk_count").Value(statistics.ChunkCount)
            .Item("chunk_list_count").Value(statistics.ChunkListCount)
            .Item("rank").Value(statistics.Rank)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void THunkChunkTreeStatistics::Accumulate(const THunkChunkTreeStatistics& other)
{
    ReferencedRegularDiskSpace += other.ReferencedRegularDiskSpace;
    ReferencedErasureDiskSpace += other.ReferencedErasureDiskSpace;
    RegularDiskSpace += other.RegularDiskSpace;
    ErasureDiskSpace += other.ErasureDiskSpace;
    ChunkCount += other.ChunkCount;
}

void THunkChunkTreeStatistics::Deaccumulate(const THunkChunkTreeStatistics& other)
{
    ReferencedRegularDiskSpace -= other.ReferencedRegularDiskSpace;
    ReferencedErasureDiskSpace -= other.ReferencedErasureDiskSpace;
    RegularDiskSpace -= other.RegularDiskSpace;
    ErasureDiskSpace -= other.ErasureDiskSpace;
    ChunkCount -= other.ChunkCount;
}

TChunkOwnerDataStatistics THunkChunkTreeStatistics::ToDataStatistics() const
{
    TChunkOwnerDataStatistics result;
    result.ChunkCount = ChunkCount;
    result.RegularDiskSpace = RegularDiskSpace;
    result.ErasureDiskSpace = ErasureDiskSpace;

    // NB: ReferencedRegularDiskSpace and ReferencedErasureDiskSpace do not go anywhere
    // because they are irrelevant in the context of data statistics.

    return result;
}

void THunkChunkTreeStatistics::Persist(const NCellMaster::TPersistenceContext& context)
{
    YT_VERIFY(context.GetVersion() >= NCellMaster::EMasterReign::HunkChunkTreeStatisticsOverhaul);

    using NYT::Persist;
    Persist(context, ReferencedRegularDiskSpace);
    Persist(context, ReferencedErasureDiskSpace);
    Persist(context, RegularDiskSpace);
    Persist(context, ErasureDiskSpace);
    Persist(context, ChunkCount);
}

bool THunkChunkTreeStatistics::operator==(const THunkChunkTreeStatistics& other) const
{
    return
        ReferencedRegularDiskSpace == other.ReferencedRegularDiskSpace &&
        ReferencedErasureDiskSpace == other.ReferencedErasureDiskSpace &&
        RegularDiskSpace == other.RegularDiskSpace &&
        ErasureDiskSpace == other.ErasureDiskSpace &&
        ChunkCount == other.ChunkCount;
}

void FormatValue(TStringBuilderBase* builder, const THunkChunkTreeStatistics& statistics, TStringBuf spec)
{
    FormatValue(builder, ConvertToYsonString(statistics, EYsonFormat::Text), spec);
}

void Serialize(const THunkChunkTreeStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("referenced_regular_disk_space").Value(statistics.ReferencedRegularDiskSpace)
            .Item("referenced_erasure_disk_space").Value(statistics.ReferencedErasureDiskSpace)
            .Item("regular_disk_space").Value(statistics.RegularDiskSpace)
            .Item("erasure_disk_space").Value(statistics.ErasureDiskSpace)
            .Item("chunk_count").Value(statistics.ChunkCount)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
