#include "chunk_reader_statistics.h"

namespace NYT {
namespace NChunkClient {

using namespace NProfiling;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkReaderStatistics* protoChunkReaderStatistics, const TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    protoChunkReaderStatistics->set_data_bytes_read_from_disk(chunkReaderStatistics->DataBytesReadFromDisk);
    protoChunkReaderStatistics->set_data_bytes_read_from_cache(chunkReaderStatistics->DataBytesReadFromCache);
    protoChunkReaderStatistics->set_meta_bytes_read_from_disk(chunkReaderStatistics->MetaBytesReadFromDisk);
}

void FromProto(TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics = New<TChunkReaderStatistics>();
    chunkReaderStatistics->DataBytesReadFromDisk = protoChunkReaderStatistics.data_bytes_read_from_disk();
    chunkReaderStatistics->DataBytesReadFromCache = protoChunkReaderStatistics.data_bytes_read_from_cache();
    chunkReaderStatistics->MetaBytesReadFromDisk = protoChunkReaderStatistics.meta_bytes_read_from_disk();
}

void UpdateFromProto(const TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    const auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics->DataBytesReadFromDisk += protoChunkReaderStatistics.data_bytes_read_from_disk();
    chunkReaderStatistics->DataBytesReadFromCache += protoChunkReaderStatistics.data_bytes_read_from_cache();
    chunkReaderStatistics->MetaBytesReadFromDisk += protoChunkReaderStatistics.meta_bytes_read_from_disk();
}

////////////////////////////////////////////////////////////////////////////////

TChunkReaderStatisticsCounters::TChunkReaderStatisticsCounters(
    const TYPath& path,
    const TTagIdList& tagIds)
    : DataBytesReadFromDisk(path + "/data_bytes_read_from_disk", tagIds)
    , DataBytesReadFromCache(path + "/data_bytes_read_from_cache", tagIds)
    , MetaBytesReadFromDisk(path + "/meta_bytes_read_from_disk", tagIds)
{ }


void TChunkReaderStatisticsCounters::Increment(
    const TProfiler& profiler,
    const TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    profiler.Increment(DataBytesReadFromDisk, chunkReaderStatistics->DataBytesReadFromDisk);
    profiler.Increment(DataBytesReadFromCache, chunkReaderStatistics->DataBytesReadFromCache);
    profiler.Increment(MetaBytesReadFromDisk, chunkReaderStatistics->MetaBytesReadFromDisk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
