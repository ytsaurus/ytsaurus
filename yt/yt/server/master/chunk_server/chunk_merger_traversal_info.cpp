#include "chunk_merger_traversal_info.h"

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

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
    MaxChunkMetaSizeViolatedCriteria += rhs.MaxChunkMetaSizeViolatedCriteria;
    MaxChunkListCountPerMergeSessionViolatedCriteria += rhs.MaxChunkListCountPerMergeSessionViolatedCriteria;
    MaxJobsPerChunkListViolatedCriteria += rhs.MaxJobsPerChunkListViolatedCriteria;
    return *this;
}

void TChunkMergerTraversalInfo::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, ChunkCount);
    PHOENIX_REGISTER_FIELD(2, ConfigVersion);
}

PHOENIX_DEFINE_TYPE(TChunkMergerTraversalInfo);

void FormatValue(TStringBuilderBase* builder, const TChunkMergerTraversalStatistics& traversalStatistics, TStringBuf /*spec*/)
{
    builder->AppendFormat("{ChunkCount: %v, ConfigVersion: %v}",
        traversalStatistics.ChunkCount,
        traversalStatistics.ConfigVersion);
}

void TChunkMergerInfo::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, TraversalInfo);
    PHOENIX_REGISTER_FIELD(2, UpdatedSinceLastMerge);
    PHOENIX_REGISTER_FIELD(3, Revision);
}

PHOENIX_DEFINE_TYPE(TChunkMergerInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
