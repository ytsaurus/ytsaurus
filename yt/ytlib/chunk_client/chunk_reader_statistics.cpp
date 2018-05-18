#include "chunk_reader_statistics.h"

namespace NYT {
namespace NChunkClient {

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
