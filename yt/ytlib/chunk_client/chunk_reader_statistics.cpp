#include "chunk_reader_statistics.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkReaderStatistics* protoChunkReaderStatistics, const TChunkReaderStatisticsPtr& chunkDiskReadStatistis)
{
    protoChunkReaderStatistics->set_data_bytes_read_from_disk(chunkDiskReadStatistis->DataBytesReadFromDisk);
    protoChunkReaderStatistics->set_data_bytes_read_from_cache(chunkDiskReadStatistis->DataBytesReadFromCache);
    protoChunkReaderStatistics->set_meta_bytes_read_from_disk(chunkDiskReadStatistis->MetaBytesReadFromDisk);
}

void FromProto(TChunkReaderStatisticsPtr* chunkDiskReadStatistisPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    auto& chunkDiskReadStatistis = *chunkDiskReadStatistisPtr;
    chunkDiskReadStatistis = New<TChunkReaderStatistics>();
    chunkDiskReadStatistis->DataBytesReadFromDisk = protoChunkReaderStatistics.data_bytes_read_from_disk();
    chunkDiskReadStatistis->DataBytesReadFromCache = protoChunkReaderStatistics.data_bytes_read_from_cache();
    chunkDiskReadStatistis->MetaBytesReadFromDisk = protoChunkReaderStatistics.meta_bytes_read_from_disk();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
