#include "chunk_reader_statistics.h"

#include <yt/yt/core/misc/statistics.h>

namespace NYT::NChunkClient {

using namespace NProfiling;
using namespace NYPath;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkReaderStatistics* protoChunkReaderStatistics, const TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    protoChunkReaderStatistics->set_data_bytes_read_from_disk(chunkReaderStatistics->DataBytesReadFromDisk.load(std::memory_order_relaxed));
    protoChunkReaderStatistics->set_data_io_requests(chunkReaderStatistics->DataIORequests.load(std::memory_order_relaxed));
    protoChunkReaderStatistics->set_data_bytes_transmitted(chunkReaderStatistics->DataBytesTransmitted.load(std::memory_order_relaxed));
    protoChunkReaderStatistics->set_data_bytes_read_from_cache(chunkReaderStatistics->DataBytesReadFromCache.load(std::memory_order_relaxed));
    protoChunkReaderStatistics->set_meta_bytes_read_from_disk(chunkReaderStatistics->MetaBytesReadFromDisk.load(std::memory_order_relaxed));
    protoChunkReaderStatistics->set_omitted_suspicious_node_count(chunkReaderStatistics->OmittedSuspiciousNodeCount.load(std::memory_order_relaxed));
    protoChunkReaderStatistics->set_data_wait_time(chunkReaderStatistics->DataWaitTime.load(std::memory_order_relaxed));
    protoChunkReaderStatistics->set_meta_wait_time(chunkReaderStatistics->MetaWaitTime.load(std::memory_order_relaxed));
    protoChunkReaderStatistics->set_meta_read_from_disk_time(chunkReaderStatistics->MetaReadFromDiskTime.load(std::memory_order_relaxed));
    protoChunkReaderStatistics->set_pick_peer_wait_time(chunkReaderStatistics->PickPeerWaitTime.load(std::memory_order_relaxed));
}

void FromProto(TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics = New<TChunkReaderStatistics>();
    chunkReaderStatistics->DataBytesReadFromDisk.store(protoChunkReaderStatistics.data_bytes_read_from_disk(), std::memory_order_relaxed);
    chunkReaderStatistics->DataIORequests.store(protoChunkReaderStatistics.data_io_requests(), std::memory_order_relaxed);
    chunkReaderStatistics->DataBytesTransmitted.store(protoChunkReaderStatistics.data_bytes_transmitted(), std::memory_order_relaxed);
    chunkReaderStatistics->DataBytesReadFromCache.store(protoChunkReaderStatistics.data_bytes_read_from_cache(), std::memory_order_relaxed);
    chunkReaderStatistics->MetaBytesReadFromDisk.store(protoChunkReaderStatistics.meta_bytes_read_from_disk(), std::memory_order_relaxed);
    chunkReaderStatistics->OmittedSuspiciousNodeCount.store(protoChunkReaderStatistics.omitted_suspicious_node_count(), std::memory_order_relaxed);
    chunkReaderStatistics->DataWaitTime.store(protoChunkReaderStatistics.data_wait_time(), std::memory_order_relaxed);
    chunkReaderStatistics->MetaWaitTime.store(protoChunkReaderStatistics.meta_wait_time(), std::memory_order_relaxed);
    chunkReaderStatistics->MetaReadFromDiskTime.store(protoChunkReaderStatistics.meta_read_from_disk_time(), std::memory_order_relaxed);
    chunkReaderStatistics->PickPeerWaitTime.store(protoChunkReaderStatistics.pick_peer_wait_time(), std::memory_order_relaxed);
}

void UpdateFromProto(const TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    const auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics->DataBytesReadFromDisk.fetch_add(protoChunkReaderStatistics.data_bytes_read_from_disk(), std::memory_order_relaxed);
    chunkReaderStatistics->DataIORequests.fetch_add(protoChunkReaderStatistics.data_io_requests(), std::memory_order_relaxed);
    chunkReaderStatistics->DataBytesTransmitted.fetch_add(protoChunkReaderStatistics.data_bytes_transmitted(), std::memory_order_relaxed);
    chunkReaderStatistics->DataBytesReadFromCache.fetch_add(protoChunkReaderStatistics.data_bytes_read_from_cache(), std::memory_order_relaxed);
    chunkReaderStatistics->MetaBytesReadFromDisk.fetch_add(protoChunkReaderStatistics.meta_bytes_read_from_disk(), std::memory_order_relaxed);
    chunkReaderStatistics->OmittedSuspiciousNodeCount.fetch_add(protoChunkReaderStatistics.omitted_suspicious_node_count(), std::memory_order_relaxed);
    chunkReaderStatistics->DataWaitTime.fetch_add(protoChunkReaderStatistics.data_wait_time(), std::memory_order_relaxed);
    chunkReaderStatistics->MetaWaitTime.fetch_add(protoChunkReaderStatistics.meta_wait_time(), std::memory_order_relaxed);
    chunkReaderStatistics->MetaReadFromDiskTime.fetch_add(protoChunkReaderStatistics.meta_read_from_disk_time(), std::memory_order_relaxed);
    chunkReaderStatistics->PickPeerWaitTime.fetch_add(protoChunkReaderStatistics.pick_peer_wait_time(), std::memory_order_relaxed);
}

void DumpChunkReaderStatistics(
    TStatistics* jobStatisitcs,
    const TString& path,
    const TChunkReaderStatisticsPtr& chunkReaderStatisticsPtr)
{
    jobStatisitcs->AddSample(path + "/data_bytes_read_from_disk", chunkReaderStatisticsPtr->DataBytesReadFromDisk.load(std::memory_order_relaxed));
    jobStatisitcs->AddSample(path + "/data_bytes_transmitted", chunkReaderStatisticsPtr->DataBytesTransmitted.load(std::memory_order_relaxed));
    jobStatisitcs->AddSample(path + "/data_bytes_read_from_cache", chunkReaderStatisticsPtr->DataBytesReadFromCache.load(std::memory_order_relaxed));
    jobStatisitcs->AddSample(path + "/meta_bytes_read_from_disk", chunkReaderStatisticsPtr->MetaBytesReadFromDisk.load(std::memory_order_relaxed));
}

void DumpTimingStatistics(
    TStatistics* jobStatistics,
    const TString& path,
    const TTimingStatistics& timingStatistics)
{
    jobStatistics->AddSample(path + "/wait_time", timingStatistics.WaitTime);
    jobStatistics->AddSample(path + "/read_time", timingStatistics.ReadTime);
    jobStatistics->AddSample(path + "/idle_time", timingStatistics.IdleTime);
}

////////////////////////////////////////////////////////////////////////////////

TChunkReaderStatisticsCounters::TChunkReaderStatisticsCounters(const NProfiling::TProfiler& profiler)
    : DataBytesReadFromDisk_(profiler.Counter("/data_bytes_read_from_disk"))
    , DataIORequests_(profiler.Counter("/data_io_requests"))
    , DataBytesTransmitted_(profiler.Counter("/data_bytes_transmitted"))
    , DataBytesReadFromCache_(profiler.Counter("/data_bytes_read_from_cache"))
    , WastedDataBytesReadFromDisk_(profiler.Counter("/wasted_data_bytes_read_from_disk"))
    , WastedDataBytesTransmitted_(profiler.Counter("/wasted_data_bytes_transmitted"))
    , WastedDataBytesReadFromCache_(profiler.Counter("/wasted_data_bytes_read_from_cache"))
    , MetaBytesReadFromDisk_(profiler.Counter("/meta_bytes_read_from_disk"))
    , OmittedSuspiciousNodeCount_(profiler.Counter("/omitted_suspicious_node_count"))
    , DataWaitTime_(profiler.TimeCounter("/data_wait_time"))
    , MetaWaitTime_(profiler.TimeCounter("/meta_wait_time"))
    , MetaReadFromDiskTime_(profiler.TimeCounter("/meta_read_from_disk_time"))
    , PickPeerWaitTime_(profiler.TimeCounter("/pick_peer_wait_time"))
{ }

void TChunkReaderStatisticsCounters::Increment(
    const TChunkReaderStatisticsPtr& chunkReaderStatistics,
    bool failed)
{
    DataBytesReadFromDisk_.Increment(chunkReaderStatistics->DataBytesReadFromDisk.load(std::memory_order_relaxed));
    DataIORequests_.Increment(chunkReaderStatistics->DataIORequests.load(std::memory_order_relaxed));
    DataBytesTransmitted_.Increment(chunkReaderStatistics->DataBytesTransmitted.load(std::memory_order_relaxed));
    DataBytesReadFromCache_.Increment(chunkReaderStatistics->DataBytesReadFromCache.load(std::memory_order_relaxed));
    if (failed) {
        WastedDataBytesReadFromDisk_.Increment(chunkReaderStatistics->DataBytesReadFromDisk.load(std::memory_order_relaxed));
        WastedDataBytesTransmitted_.Increment(chunkReaderStatistics->DataBytesTransmitted.load(std::memory_order_relaxed));
        WastedDataBytesReadFromCache_.Increment(chunkReaderStatistics->DataBytesReadFromCache.load(std::memory_order_relaxed));
    }

    MetaBytesReadFromDisk_.Increment(chunkReaderStatistics->MetaBytesReadFromDisk.load(std::memory_order_relaxed));
    OmittedSuspiciousNodeCount_.Increment(chunkReaderStatistics->OmittedSuspiciousNodeCount.load(std::memory_order_relaxed));

    DataWaitTime_.Add(TDuration::FromValue(chunkReaderStatistics->DataWaitTime.load(std::memory_order_relaxed)));
    MetaWaitTime_.Add(TDuration::FromValue(chunkReaderStatistics->MetaWaitTime.load(std::memory_order_relaxed)));
    MetaReadFromDiskTime_.Add(TDuration::FromValue(chunkReaderStatistics->MetaReadFromDiskTime.load(std::memory_order_relaxed)));
    PickPeerWaitTime_.Add(TDuration::FromValue(chunkReaderStatistics->PickPeerWaitTime.load(std::memory_order_relaxed)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
