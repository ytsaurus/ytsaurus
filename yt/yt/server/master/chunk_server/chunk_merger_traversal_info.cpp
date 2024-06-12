#include "chunk_merger_traversal_info.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TChunkMergerViolatedCriteriaStatistics& TChunkMergerViolatedCriteriaStatistics::operator+=(
    const TChunkMergerViolatedCriteriaStatistics& rhs)
{
    MaxChunkCountViolatedCriteria += rhs.MaxChunkCountViolatedCriteria;
    MaxRowCountViolatedCriteria += rhs.MaxRowCountViolatedCriteria;
    MaxDataWeightViolatedCriteria += rhs.MaxDataWeightViolatedCriteria;
    MaxUncompressedDataSizeViolatedCriteria += rhs.MaxUncompressedDataSizeViolatedCriteria;
    MaxCompressedDataSizeViolatedCriteria += rhs.MaxCompressedDataSizeViolatedCriteria;
    MaxInputChunkDataWeightViolatedCriteria += rhs.MaxInputChunkDataWeightViolatedCriteria;
    return *this;
}

void TChunkMergerTraversalInfo::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, ChunkCount);
    Save(context, ConfigVersion);
}

void TChunkMergerTraversalInfo::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, ChunkCount);
    if (context.GetVersion() >= EMasterReign::RemovedRedundantStatisticsFromChunkOwnerBase) {
        Load(context, ConfigVersion);
    } else {
        ConfigVersion = Load<i64>(context);
    }
}

void FormatValue(TStringBuilderBase* builder, const TChunkMergerTraversalStatistics& traversalStatistics, TStringBuf /*spec*/)
{
    builder->AppendFormat("{ChunkCount: %v, ConfigVersion: %v}",
        traversalStatistics.ChunkCount,
        traversalStatistics.ConfigVersion);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
