#include "chunk_reader_statistics.h"

#include <yt/core/misc/statistics.h>

namespace NYT::NChunkClient {

using namespace NProfiling;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkReaderStatistics* protoChunkReaderStatistics, const TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    protoChunkReaderStatistics->set_data_bytes_read_from_disk(chunkReaderStatistics->DataBytesReadFromDisk);
    protoChunkReaderStatistics->set_data_bytes_transmitted(chunkReaderStatistics->DataBytesTransmitted);
    protoChunkReaderStatistics->set_data_bytes_read_from_uncompressed_cache(chunkReaderStatistics->DataBytesReadFromUncompressedCache);
    protoChunkReaderStatistics->set_data_bytes_read_from_cache(chunkReaderStatistics->DataBytesReadFromCache);
    protoChunkReaderStatistics->set_meta_bytes_read_from_disk(chunkReaderStatistics->MetaBytesReadFromDisk);
    protoChunkReaderStatistics->set_data_wait_time(chunkReaderStatistics->DataWaitTime);
    protoChunkReaderStatistics->set_meta_wait_time(chunkReaderStatistics->MetaWaitTime);
    protoChunkReaderStatistics->set_meta_read_from_disk_time(chunkReaderStatistics->MetaReadFromDiskTime);
    protoChunkReaderStatistics->set_pick_peer_wait_time(chunkReaderStatistics->PickPeerWaitTime);
}

void FromProto(TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics = New<TChunkReaderStatistics>();
    chunkReaderStatistics->DataBytesReadFromDisk = protoChunkReaderStatistics.data_bytes_read_from_disk();
    chunkReaderStatistics->DataBytesTransmitted = protoChunkReaderStatistics.data_bytes_transmitted();
    chunkReaderStatistics->DataBytesReadFromUncompressedCache = protoChunkReaderStatistics.data_bytes_read_from_uncompressed_cache();
    chunkReaderStatistics->DataBytesReadFromCache = protoChunkReaderStatistics.data_bytes_read_from_cache();
    chunkReaderStatistics->MetaBytesReadFromDisk = protoChunkReaderStatistics.meta_bytes_read_from_disk();
    chunkReaderStatistics->DataWaitTime = protoChunkReaderStatistics.data_wait_time();
    chunkReaderStatistics->MetaWaitTime = protoChunkReaderStatistics.meta_wait_time();
    chunkReaderStatistics->MetaReadFromDiskTime = protoChunkReaderStatistics.meta_read_from_disk_time();
    chunkReaderStatistics->PickPeerWaitTime = protoChunkReaderStatistics.pick_peer_wait_time();
}

void UpdateFromProto(const TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    const auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics->DataBytesReadFromDisk += protoChunkReaderStatistics.data_bytes_read_from_disk();
    chunkReaderStatistics->DataBytesTransmitted += protoChunkReaderStatistics.data_bytes_transmitted();
    chunkReaderStatistics->DataBytesReadFromUncompressedCache += protoChunkReaderStatistics.data_bytes_read_from_uncompressed_cache();
    chunkReaderStatistics->DataBytesReadFromCache += protoChunkReaderStatistics.data_bytes_read_from_cache();
    chunkReaderStatistics->MetaBytesReadFromDisk += protoChunkReaderStatistics.meta_bytes_read_from_disk();
    chunkReaderStatistics->DataWaitTime += protoChunkReaderStatistics.data_wait_time();
    chunkReaderStatistics->MetaWaitTime += protoChunkReaderStatistics.meta_wait_time();
    chunkReaderStatistics->MetaReadFromDiskTime += protoChunkReaderStatistics.meta_read_from_disk_time();
    chunkReaderStatistics->PickPeerWaitTime += protoChunkReaderStatistics.pick_peer_wait_time();
}

void DumpChunkReaderStatistics(
    TStatistics* jobStatisitcs,
    const TString& path,
    const TChunkReaderStatisticsPtr& chunkReaderStatisticsPtr)
{
    jobStatisitcs->AddSample(path + "/data_bytes_read_from_disk", chunkReaderStatisticsPtr->DataBytesReadFromDisk);
    jobStatisitcs->AddSample(path + "/data_bytes_transmitted", chunkReaderStatisticsPtr->DataBytesTransmitted);
    jobStatisitcs->AddSample(path + "/data_bytes_read_from_uncompressed_cache", chunkReaderStatisticsPtr->DataBytesReadFromUncompressedCache);
    jobStatisitcs->AddSample(path + "/data_bytes_read_from_cache", chunkReaderStatisticsPtr->DataBytesReadFromCache);
    jobStatisitcs->AddSample(path + "/meta_bytes_read_from_disk", chunkReaderStatisticsPtr->MetaBytesReadFromDisk);
}

////////////////////////////////////////////////////////////////////////////////

TChunkReaderStatisticsCounters::TChunkReaderStatisticsCounters(
    const TYPath& path,
    const TTagIdList& tagIds)
    : DataBytesReadFromDisk(path + "/data_bytes_read_from_disk", tagIds)
    , DataBytesTransmitted(path + "/data_bytes_transmitted", tagIds)
    , DataBytesReadFromUncompressedCache(path + "/data_bytes_read_from_uncompressed_cache", tagIds)
    , DataBytesReadFromCache(path + "/data_bytes_read_from_cache", tagIds)
    , MetaBytesReadFromDisk(path + "/meta_bytes_read_from_disk", tagIds)
    , DataWaitTime(path + "/data_wait_time", tagIds)
    , MetaWaitTime(path + "/meta_wait_time", tagIds)
    , MetaReadFromDiskTime(path + "/meta_read_from_disk_time", tagIds)
    , PickPeerWaitTime(path + "/pick_peer_wait_time", tagIds)
{ }


void TChunkReaderStatisticsCounters::Increment(
    const TProfiler& profiler,
    const TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    profiler.Increment(DataBytesReadFromDisk, chunkReaderStatistics->DataBytesReadFromDisk);
    profiler.Increment(DataBytesTransmitted, chunkReaderStatistics->DataBytesTransmitted);
    profiler.Increment(DataBytesReadFromUncompressedCache, chunkReaderStatistics->DataBytesReadFromUncompressedCache);
    profiler.Increment(DataBytesReadFromCache, chunkReaderStatistics->DataBytesReadFromCache);
    profiler.Increment(MetaBytesReadFromDisk, chunkReaderStatistics->MetaBytesReadFromDisk);
    profiler.Increment(DataWaitTime, chunkReaderStatistics->DataWaitTime);
    profiler.Increment(MetaWaitTime, chunkReaderStatistics->MetaWaitTime);
    profiler.Increment(MetaReadFromDiskTime, chunkReaderStatistics->MetaReadFromDiskTime);
    profiler.Increment(PickPeerWaitTime, chunkReaderStatistics->PickPeerWaitTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
