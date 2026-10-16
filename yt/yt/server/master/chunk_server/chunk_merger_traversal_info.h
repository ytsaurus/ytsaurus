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

struct TChunkMergerInfo
{
    TChunkMergerTraversalInfo TraversalInfo;
    // If chunk owner is changed, while it is being merged, it should be marked updated
    // to initiate another merge after the current one is finished.
    bool UpdatedSinceLastMerge = false;
    i64 Revision = 0;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
