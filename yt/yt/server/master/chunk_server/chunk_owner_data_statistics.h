#pragma once

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkOwnerDataStatistics
{
    i64 UncompressedDataSize = 0;
    i64 CompressedDataSize = 0;
    i64 RowCount = 0;
    i64 ChunkCount = 0;
    i64 RegularDiskSpace = 0;
    i64 ErasureDiskSpace = 0;
    i64 DataWeight = 0;

    bool operator==(const TChunkOwnerDataStatistics& other) const;
    TChunkOwnerDataStatistics& operator+=(const TChunkOwnerDataStatistics& rhs);
    TChunkOwnerDataStatistics operator+(const TChunkOwnerDataStatistics& rhs) const;

    bool IsDataWeightValid() const;

    template <class TContext>
    void Save(TContext& context) const;
    template <class TContext>
    void Load(TContext& context);
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TChunkOwnerDataStatistics& statistics, NYson::IYsonConsumer* consumer);
void FormatValue(TStringBuilderBase* builder, const TChunkOwnerDataStatistics& statistics, TStringBuf spec);
TString ToString(const TChunkOwnerDataStatistics& statistics);

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TChunkOwnerDataStatistics* dataStatistics,
    const NChunkClient::NProto::TDataStatistics& protoDataStatistics);

void ToProto(
    NChunkClient::NProto::TDataStatistics* protoDataStatistics,
    const TChunkOwnerDataStatistics& dataStatistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define CHUNK_OWNER_DATA_STATISTICS_
#include "chunk_owner_data_statistics-inl.h"
#undef CHUNK_OWNER_DATA_STATISTICS_
