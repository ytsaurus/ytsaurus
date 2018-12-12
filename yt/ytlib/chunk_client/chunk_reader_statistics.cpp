#include "chunk_reader_statistics.h"

#include <yt/ytlib/job_tracker_client/statistics.h>

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
    protoChunkReaderStatistics->set_data_wait_time(chunkReaderStatistics->DataWaitTime);
    protoChunkReaderStatistics->set_meta_wait_time(chunkReaderStatistics->MetaWaitTime);
    protoChunkReaderStatistics->set_pick_peer_wait_time(chunkReaderStatistics->PickPeerWaitTime);
}

void FromProto(TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics = New<TChunkReaderStatistics>();
    chunkReaderStatistics->DataBytesReadFromDisk = protoChunkReaderStatistics.data_bytes_read_from_disk();
    chunkReaderStatistics->DataBytesReadFromCache = protoChunkReaderStatistics.data_bytes_read_from_cache();
    chunkReaderStatistics->MetaBytesReadFromDisk = protoChunkReaderStatistics.meta_bytes_read_from_disk();
    chunkReaderStatistics->DataWaitTime = protoChunkReaderStatistics.data_wait_time();
    chunkReaderStatistics->MetaWaitTime = protoChunkReaderStatistics.meta_wait_time();
    chunkReaderStatistics->PickPeerWaitTime = protoChunkReaderStatistics.pick_peer_wait_time();
}

void UpdateFromProto(const TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    const auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics->DataBytesReadFromDisk += protoChunkReaderStatistics.data_bytes_read_from_disk();
    chunkReaderStatistics->DataBytesReadFromCache += protoChunkReaderStatistics.data_bytes_read_from_cache();
    chunkReaderStatistics->MetaBytesReadFromDisk += protoChunkReaderStatistics.meta_bytes_read_from_disk();
    chunkReaderStatistics->DataWaitTime += protoChunkReaderStatistics.data_wait_time();
    chunkReaderStatistics->MetaWaitTime += protoChunkReaderStatistics.meta_wait_time();
    chunkReaderStatistics->PickPeerWaitTime += protoChunkReaderStatistics.pick_peer_wait_time();
}

void DumpChunkReaderStatistics(
    NJobTrackerClient::TStatistics* jobStatisitcs,
    const TString& path,
    const TChunkReaderStatisticsPtr& chunkReaderStatisticsPtr)
{
    jobStatisitcs->AddSample(path + "/data_bytes_read_from_disk", chunkReaderStatisticsPtr->DataBytesReadFromDisk);
    jobStatisitcs->AddSample(path + "/data_bytes_read_from_cache", chunkReaderStatisticsPtr->DataBytesReadFromCache);
    jobStatisitcs->AddSample(path + "/meta_bytes_read_from_disk", chunkReaderStatisticsPtr->MetaBytesReadFromDisk);
}

////////////////////////////////////////////////////////////////////////////////

TChunkReaderStatisticsCounters::TChunkReaderStatisticsCounters(
    const TYPath& path,
    const TTagIdList& tagIds)
    : DataBytesReadFromDisk(path + "/data_bytes_read_from_disk", tagIds)
    , DataBytesReadFromCache(path + "/data_bytes_read_from_cache", tagIds)
    , MetaBytesReadFromDisk(path + "/meta_bytes_read_from_disk", tagIds)
    , DataWaitTime(path + "/data_wait_time", tagIds)
    , MetaWaitTime(path + "/meta_wait_time", tagIds)
    , PickPeerWaitTime(path + "/pick_peer_wait_time", tagIds)
{ }


void TChunkReaderStatisticsCounters::Increment(
    const TProfiler& profiler,
    const TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    profiler.Increment(DataBytesReadFromDisk, chunkReaderStatistics->DataBytesReadFromDisk);
    profiler.Increment(DataBytesReadFromCache, chunkReaderStatistics->DataBytesReadFromCache);
    profiler.Increment(MetaBytesReadFromDisk, chunkReaderStatistics->MetaBytesReadFromDisk);
    profiler.Increment(DataWaitTime, chunkReaderStatistics->DataWaitTime);
    profiler.Increment(MetaWaitTime, chunkReaderStatistics->MetaWaitTime);
    profiler.Increment(PickPeerWaitTime, chunkReaderStatistics->PickPeerWaitTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
