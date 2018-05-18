#pragma once

#include "public.h"
#include <yt/ytlib/chunk_client/chunk_reader_statistics.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderStatistics
    : public TRefCounted
{
    std::atomic<i64> DataBytesReadFromDisk{0};
    std::atomic<i64> DataBytesReadFromCache{0};
    std::atomic<i64> MetaBytesReadFromDisk{0};
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderStatistics)

void ToProto(NProto::TChunkReaderStatistics* protoChunkReaderStatistics, const TChunkReaderStatisticsPtr& chunkDiskReadStatistis);
void FromProto(TChunkReaderStatisticsPtr chunkDiskReadStatistis, NProto::TChunkReaderStatistics* protoChunkReaderStatistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

