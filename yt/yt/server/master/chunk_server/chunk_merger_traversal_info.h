#pragma once

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkMergerViolatedCriteriaStatistics
{
    i64 MaxChunkCountViolatedCriteria = 0;
    i64 MaxRowCountViolatedCriteria = 0;
    i64 MaxDataWeightViolatedCriteria = 0;
    i64 MaxUncompressedDataSizeViolatedCriteria = 0;
    i64 MaxCompressedDataSizeViolatedCriteria = 0;
    i64 MaxInputChunkDataWeightViolatedCriteria = 0;
    i64 MaxChunkMetaSizeViolatedCriteria = 0;
    i64 MaxChunkListCountPerMergeSessionViolatedCriteria = 0;
    i64 MaxJobsPerChunkListViolatedCriteria = 0;

    TChunkMergerViolatedCriteriaStatistics& operator+=(const TChunkMergerViolatedCriteriaStatistics& rhs);
};

struct TChunkMergerTraversalInfo
{
    int ChunkCount = 0;
    int ConfigVersion = 0;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

struct TChunkMergerTraversalStatistics
    : public TChunkMergerTraversalInfo
{
    TChunkMergerViolatedCriteriaStatistics ViolatedCriteriaStatistics;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TChunkMergerTraversalStatistics& traversalStatistics,
    TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
