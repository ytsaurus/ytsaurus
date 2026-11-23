#pragma once

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_decl.h>

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

    // NB: Stream contexts make the type persistable via both snapshot and copy contexts.
    using TSaveContext = TStreamSaveContext;
    using TLoadContext = TStreamLoadContext;

    PHOENIX_DECLARE_TYPE(TChunkOwnerDataStatistics, 0xf6244c17);
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TChunkOwnerDataStatistics& statistics, NYson::IYsonConsumer* consumer);
void FormatValue(TStringBuilderBase* builder, const TChunkOwnerDataStatistics& statistics, TStringBuf spec);
std::string ToString(const TChunkOwnerDataStatistics& statistics);

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TChunkOwnerDataStatistics* dataStatistics,
    const NChunkClient::NProto::TDataStatistics& protoDataStatistics);

void ToProto(
    NChunkClient::NProto::TDataStatistics* protoDataStatistics,
    const TChunkOwnerDataStatistics& dataStatistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
