#pragma once

#include <yt/yt/server/master/cell_master/serialize.h>


namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkMergerViolatedCriteriaStatistics
{
    int MaxChunkCountViolatedCriteria = 0;
    int MaxRowCountViolatedCriteria = 0;
    int MaxDataWeightViolatedCriteria = 0;
    int MaxUncompressedDataSizeViolatedCriteria = 0;
    int MaxCompressedDataSizeViolatedCriteria = 0;
    int MaxInputChunkDataWeightViolatedCriteria = 0;
    int MaxChunkMetaViolatedCriteria = 0;

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
