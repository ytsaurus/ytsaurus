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

    TChunkMergerViolatedCriteriaStatistics& operator+=(const TChunkMergerViolatedCriteriaStatistics& rhs)
    {
        MaxChunkCountViolatedCriteria += rhs.MaxChunkCountViolatedCriteria;
        MaxRowCountViolatedCriteria += rhs.MaxRowCountViolatedCriteria;
        MaxDataWeightViolatedCriteria += rhs.MaxDataWeightViolatedCriteria;
        MaxUncompressedDataSizeViolatedCriteria += rhs.MaxUncompressedDataSizeViolatedCriteria;
        MaxCompressedDataSizeViolatedCriteria += rhs.MaxCompressedDataSizeViolatedCriteria;
        MaxInputChunkDataWeightViolatedCriteria += rhs.MaxInputChunkDataWeightViolatedCriteria;
        return *this;
    }
};

struct TChunkMergerTraversalInfo
{
    int ChunkCount = 0;
    i64 ConfigVersion = 0;

    TChunkMergerViolatedCriteriaStatistics ViolatedCriteriaStatistics;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TChunkMergerTraversalInfo& traversalInfo,
    TStringBuf spec);
TString ToString(const TChunkMergerTraversalInfo& traversalInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
