#pragma once

#include <yt/yt/server/master/cell_master/serialize.h>


namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkMergerTraversalInfo
{
    int ChunkCount = 0;
    i64 ConfigVersion = 0;

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
