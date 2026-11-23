#include "chunk_tree_statistics.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/phoenix/type_def.h>

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
    LogicalHunkDataWeight += other.LogicalHunkDataWeight;
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
    LogicalHunkDataWeight -= other.LogicalHunkDataWeight;
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

void TChunkTreeStatistics::RegisterMetadata(auto&& registrar)
{
    // COMPAT(akozhikhov)
    static constexpr auto HunkStatisticsFilter = [] (EMasterReign version) {
        return
            (version >= EMasterReign::HunkChunkTreeStatisticsOverhaul && version < EMasterReign::Start_26_2) ||
            version >= EMasterReign::HunkChunkTreeStatisticsOverhaul_26_2;
    };

    PHOENIX_REGISTER_FIELD(1, RowCount);
    PHOENIX_REGISTER_FIELD(2, LogicalRowCount);
    PHOENIX_REGISTER_FIELD(3, UncompressedDataSize);
    PHOENIX_REGISTER_FIELD(4, CompressedDataSize);
    PHOENIX_REGISTER_FIELD(5, DataWeight);
    PHOENIX_REGISTER_FIELD(6, LogicalDataWeight);
    PHOENIX_REGISTER_FIELD(7, RegularDiskSpace);
    PHOENIX_REGISTER_FIELD(8, ErasureDiskSpace);
    PHOENIX_REGISTER_FIELD(9, HunkDataWeight,
        .InVersions(HunkStatisticsFilter));
    PHOENIX_REGISTER_FIELD(10, HunkDataSize,
        .InVersions(HunkStatisticsFilter));
    PHOENIX_REGISTER_FIELD(11, HunkRegularDiskSpace,
        .InVersions(HunkStatisticsFilter));
    PHOENIX_REGISTER_FIELD(12, HunkErasureDiskSpace,
        .InVersions(HunkStatisticsFilter));
    PHOENIX_REGISTER_FIELD(13, LogicalHunkDataWeight,
        .SinceVersion(EMasterReign::LogicalHunkDataWeight)
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            // COMPAT(akozhikhov): Best effort estimate: hunk data weight of the rows that were
            // trimmed before the update is irrecoverable.
            this_->LogicalHunkDataWeight = this_->HunkDataWeight;
        }));
    PHOENIX_REGISTER_FIELD(14, ChunkCount);
    PHOENIX_REGISTER_FIELD(15, ChunkListCount);
    PHOENIX_REGISTER_FIELD(16, Rank);
}

PHOENIX_DEFINE_TYPE(TChunkTreeStatistics);

bool TChunkTreeStatistics::operator==(const TChunkTreeStatistics& other) const
{
    // NB: We do not compare HunkErasureDiskSpace field because it is unreliable
    // due to integer arithmetics in ComputeDiskSpaceFromDataSize.
    return
        RowCount == other.RowCount &&
        LogicalRowCount == other.LogicalRowCount &&
        UncompressedDataSize == other.UncompressedDataSize &&
        CompressedDataSize == other.CompressedDataSize &&
        RegularDiskSpace == other.RegularDiskSpace &&
        ErasureDiskSpace == other.ErasureDiskSpace &&
        HunkDataWeight == other.HunkDataWeight &&
        LogicalHunkDataWeight == other.LogicalHunkDataWeight &&
        HunkDataSize == other.HunkDataSize &&
        HunkRegularDiskSpace == other.HunkRegularDiskSpace &&
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
            .Item("logical_hunk_data_weight").Value(statistics.LogicalHunkDataWeight)
            .Item("trimmed_hunk_data_weight").Value(statistics.LogicalHunkDataWeight - statistics.HunkDataWeight)
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

void THunkChunkTreeStatistics::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, ReferencedRegularDiskSpace);
    PHOENIX_REGISTER_FIELD(2, ReferencedErasureDiskSpace);
    PHOENIX_REGISTER_FIELD(3, RegularDiskSpace);
    PHOENIX_REGISTER_FIELD(4, ErasureDiskSpace);
    PHOENIX_REGISTER_FIELD(5, ChunkCount);

    registrar.AfterLoad([] (TThis* /*this_*/, auto& context) {
        YT_VERIFY(context.GetVersion() >= EMasterReign::HunkChunkTreeStatisticsOverhaul);
    });
}

PHOENIX_DEFINE_TYPE(THunkChunkTreeStatistics);

bool THunkChunkTreeStatistics::operator==(const THunkChunkTreeStatistics& other) const
{
    // NB: We do not compare ReferencedErasureDiskSpace field because it is unreliable
    // due to integer division in ComputeDiskSpaceFromDataSize.
    return
        ReferencedRegularDiskSpace == other.ReferencedRegularDiskSpace &&
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

void FormatValue(TStringBuilderBase* builder, const TChunkReplicationStatistics& statistics, TStringBuf spec)
{
    FormatValue(builder, ConvertToYsonString(statistics, EYsonFormat::Text), spec);
}

void Serialize(const TChunkReplicationStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total").Value(statistics.Total)
            .Item("available").Value(statistics.Available)
            .Item("lost").Value(statistics.Lost)
            .Item("data_missing").Value(statistics.DataMissing)
            .Item("parity_missing").Value(statistics.ParityMissing)
            .Item("quorum_missing").Value(statistics.QuorumMissing)
            .Item("medium_wise_lost").Value(statistics.MediumWiseLost)
            .Item("dead").Value(statistics.Dead)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
