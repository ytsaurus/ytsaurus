#include "chunk_reader_statistics.h"

#include <yt/yt/core/misc/statistics.h>
#include <yt/yt/core/misc/statistic_path.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/profiling/solomon/sensor.h>

namespace NYT::NChunkClient {

using namespace NProfiling;
using namespace NStatisticPath;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

NProfiling::IHistogramPtr TChunkReaderStatistics::CreateRequestTimeHistogram()
{
    TSensorOptions options;
    options.HistogramMax = MaxTrackedLatency;
    options.HistogramMin = MinTrackedLatency;

    return New<NProfiling::THistogram>(options);
}

void TChunkReaderStatistics::RecordDataWaitTime(TDuration duration)
{
    DataWaitTime.fetch_add(DurationToValue(duration), std::memory_order::relaxed);
    DataWaitTimeHistogram->Add(duration.SecondsFloat(), 1);
}

void TChunkReaderStatistics::RecordMetaWaitTime(TDuration duration)
{
    MetaWaitTime.fetch_add(DurationToValue(duration), std::memory_order::relaxed);
    MetaWaitTimeHistogram->Add(duration.SecondsFloat(), 1);
}

void TChunkReaderStatistics::RecordSession()
{
    SessionCount.fetch_add(1, std::memory_order::relaxed);
}

void TChunkReaderStatistics::RecordRetry()
{
    RetryCount.fetch_add(1, std::memory_order::relaxed);
}

void TChunkReaderStatistics::RecordPass()
{
    PassCount.fetch_add(1, std::memory_order::relaxed);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkReaderStatistics* protoChunkReaderStatistics, const TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    protoChunkReaderStatistics->set_data_bytes_read_from_disk(chunkReaderStatistics->DataBytesReadFromDisk.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_data_blocks_read_from_disk(chunkReaderStatistics->DataBlocksReadFromDisk.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_data_io_requests(chunkReaderStatistics->DataIORequests.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_data_bytes_transmitted(chunkReaderStatistics->DataBytesTransmitted.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_data_bytes_read_from_cache(chunkReaderStatistics->DataBytesReadFromCache.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_wasted_data_bytes_read_from_disk(chunkReaderStatistics->WastedDataBytesReadFromDisk.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_wasted_data_blocks_read_from_disk(chunkReaderStatistics->WastedDataBlocksReadFromDisk.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_meta_bytes_transmitted(chunkReaderStatistics->MetaBytesTransmitted.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_meta_bytes_read_from_disk(chunkReaderStatistics->MetaBytesReadFromDisk.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_meta_io_requests(chunkReaderStatistics->MetaIORequests.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_omitted_suspicious_node_count(chunkReaderStatistics->OmittedSuspiciousNodeCount.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_p2p_activation_count(chunkReaderStatistics->P2PActivationCount.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_remote_cpu_time(chunkReaderStatistics->RemoteCpuTime.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_data_wait_time(chunkReaderStatistics->DataWaitTime.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_meta_wait_time(chunkReaderStatistics->MetaWaitTime.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_meta_read_from_disk_time(chunkReaderStatistics->MetaReadFromDiskTime.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_pick_peer_wait_time(chunkReaderStatistics->PickPeerWaitTime.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_session_count(chunkReaderStatistics->SessionCount.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_retry_count(chunkReaderStatistics->RetryCount.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_pass_count(chunkReaderStatistics->PassCount.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_block_count(chunkReaderStatistics->BlockCount.load(std::memory_order::relaxed));
    protoChunkReaderStatistics->set_prefetched_block_count(chunkReaderStatistics->PrefetchedBlockCount.load(std::memory_order::relaxed));
}

void FromProto(TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics = New<TChunkReaderStatistics>();
    chunkReaderStatistics->DataBytesReadFromDisk.store(protoChunkReaderStatistics.data_bytes_read_from_disk(), std::memory_order::relaxed);
    chunkReaderStatistics->DataIORequests.store(protoChunkReaderStatistics.data_io_requests(), std::memory_order::relaxed);
    chunkReaderStatistics->DataBlocksReadFromDisk.store(protoChunkReaderStatistics.data_blocks_read_from_disk(), std::memory_order::relaxed);
    chunkReaderStatistics->DataBytesTransmitted.store(protoChunkReaderStatistics.data_bytes_transmitted(), std::memory_order::relaxed);
    chunkReaderStatistics->DataBytesReadFromCache.store(protoChunkReaderStatistics.data_bytes_read_from_cache(), std::memory_order::relaxed);
    chunkReaderStatistics->WastedDataBytesReadFromDisk.store(protoChunkReaderStatistics.wasted_data_bytes_read_from_disk(), std::memory_order::relaxed);
    chunkReaderStatistics->WastedDataBlocksReadFromDisk.store(protoChunkReaderStatistics.wasted_data_blocks_read_from_disk(), std::memory_order::relaxed);
    chunkReaderStatistics->MetaBytesTransmitted.store(protoChunkReaderStatistics.meta_bytes_transmitted(), std::memory_order::relaxed);
    chunkReaderStatistics->MetaBytesReadFromDisk.store(protoChunkReaderStatistics.meta_bytes_read_from_disk(), std::memory_order::relaxed);
    chunkReaderStatistics->MetaIORequests.store(protoChunkReaderStatistics.meta_io_requests(), std::memory_order::relaxed);
    chunkReaderStatistics->OmittedSuspiciousNodeCount.store(protoChunkReaderStatistics.omitted_suspicious_node_count(), std::memory_order::relaxed);
    chunkReaderStatistics->P2PActivationCount.store(protoChunkReaderStatistics.p2p_activation_count(), std::memory_order::relaxed);
    chunkReaderStatistics->RemoteCpuTime.store(protoChunkReaderStatistics.remote_cpu_time(), std::memory_order::relaxed);
    chunkReaderStatistics->DataWaitTime.store(protoChunkReaderStatistics.data_wait_time(), std::memory_order::relaxed);
    chunkReaderStatistics->MetaWaitTime.store(protoChunkReaderStatistics.meta_wait_time(), std::memory_order::relaxed);
    chunkReaderStatistics->MetaReadFromDiskTime.store(protoChunkReaderStatistics.meta_read_from_disk_time(), std::memory_order::relaxed);
    chunkReaderStatistics->PickPeerWaitTime.store(protoChunkReaderStatistics.pick_peer_wait_time(), std::memory_order::relaxed);
    chunkReaderStatistics->SessionCount.store(protoChunkReaderStatistics.session_count(), std::memory_order::relaxed);
    chunkReaderStatistics->RetryCount.store(protoChunkReaderStatistics.retry_count(), std::memory_order::relaxed);
    chunkReaderStatistics->PassCount.store(protoChunkReaderStatistics.pass_count(), std::memory_order::relaxed);
    chunkReaderStatistics->BlockCount.store(protoChunkReaderStatistics.block_count(), std::memory_order::relaxed);
    chunkReaderStatistics->PrefetchedBlockCount.store(protoChunkReaderStatistics.prefetched_block_count(), std::memory_order::relaxed);
}

void UpdateFromProto(const TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    const auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics->DataBytesReadFromDisk.fetch_add(protoChunkReaderStatistics.data_bytes_read_from_disk(), std::memory_order::relaxed);
    chunkReaderStatistics->DataIORequests.fetch_add(protoChunkReaderStatistics.data_io_requests(), std::memory_order::relaxed);
    chunkReaderStatistics->DataBlocksReadFromDisk.fetch_add(protoChunkReaderStatistics.data_blocks_read_from_disk(), std::memory_order::relaxed);
    chunkReaderStatistics->DataBytesTransmitted.fetch_add(protoChunkReaderStatistics.data_bytes_transmitted(), std::memory_order::relaxed);
    chunkReaderStatistics->DataBytesReadFromCache.fetch_add(protoChunkReaderStatistics.data_bytes_read_from_cache(), std::memory_order::relaxed);
    chunkReaderStatistics->WastedDataBytesReadFromDisk.fetch_add(protoChunkReaderStatistics.wasted_data_bytes_read_from_disk(), std::memory_order::relaxed);
    chunkReaderStatistics->WastedDataBlocksReadFromDisk.fetch_add(protoChunkReaderStatistics.wasted_data_blocks_read_from_disk(), std::memory_order::relaxed);
    chunkReaderStatistics->MetaBytesTransmitted.fetch_add(protoChunkReaderStatistics.meta_bytes_transmitted(), std::memory_order::relaxed);
    chunkReaderStatistics->MetaBytesReadFromDisk.fetch_add(protoChunkReaderStatistics.meta_bytes_read_from_disk(), std::memory_order::relaxed);
    chunkReaderStatistics->MetaIORequests.fetch_add(protoChunkReaderStatistics.meta_io_requests(), std::memory_order::relaxed);
    chunkReaderStatistics->OmittedSuspiciousNodeCount.fetch_add(protoChunkReaderStatistics.omitted_suspicious_node_count(), std::memory_order::relaxed);
    chunkReaderStatistics->P2PActivationCount.fetch_add(protoChunkReaderStatistics.p2p_activation_count(), std::memory_order::relaxed);
    chunkReaderStatistics->RemoteCpuTime.fetch_add(protoChunkReaderStatistics.remote_cpu_time(), std::memory_order::relaxed);
    chunkReaderStatistics->DataWaitTime.fetch_add(protoChunkReaderStatistics.data_wait_time(), std::memory_order::relaxed);
    chunkReaderStatistics->MetaWaitTime.fetch_add(protoChunkReaderStatistics.meta_wait_time(), std::memory_order::relaxed);
    chunkReaderStatistics->MetaReadFromDiskTime.fetch_add(protoChunkReaderStatistics.meta_read_from_disk_time(), std::memory_order::relaxed);
    chunkReaderStatistics->PickPeerWaitTime.fetch_add(protoChunkReaderStatistics.pick_peer_wait_time(), std::memory_order::relaxed);
    chunkReaderStatistics->SessionCount.fetch_add(protoChunkReaderStatistics.session_count(), std::memory_order::relaxed);
    chunkReaderStatistics->RetryCount.fetch_add(protoChunkReaderStatistics.retry_count(), std::memory_order::relaxed);
    chunkReaderStatistics->PassCount.fetch_add(protoChunkReaderStatistics.pass_count(), std::memory_order::relaxed);
    chunkReaderStatistics->BlockCount.fetch_add(protoChunkReaderStatistics.block_count(), std::memory_order::relaxed);
    chunkReaderStatistics->PrefetchedBlockCount.fetch_add(protoChunkReaderStatistics.prefetched_block_count(), std::memory_order::relaxed);
}

void DumpChunkReaderStatistics(
    TStatistics* jobStatistics,
    const TStatisticPath& prefixPath,
    const TChunkReaderStatisticsPtr& chunkReaderStatisticsPtr)
{
    jobStatistics->AddSample(prefixPath / "data_bytes_read_from_disk"_L, chunkReaderStatisticsPtr->DataBytesReadFromDisk.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "data_io_requests"_L, chunkReaderStatisticsPtr->DataIORequests.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "data_blocks_read_from_disk"_L, chunkReaderStatisticsPtr->DataBlocksReadFromDisk.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "data_bytes_transmitted"_L, chunkReaderStatisticsPtr->DataBytesTransmitted.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "data_bytes_read_from_cache"_L, chunkReaderStatisticsPtr->DataBytesReadFromCache.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "wasted_data_bytes_read_from_disk"_L, chunkReaderStatisticsPtr->WastedDataBytesReadFromDisk.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "wasted_data_blocks_read_from_disk"_L, chunkReaderStatisticsPtr->WastedDataBlocksReadFromDisk.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "meta_bytes_transmitted"_L, chunkReaderStatisticsPtr->MetaBytesTransmitted.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "meta_bytes_read_from_disk"_L, chunkReaderStatisticsPtr->MetaBytesReadFromDisk.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "meta_io_requests"_L, chunkReaderStatisticsPtr->MetaIORequests.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "session_count"_L, chunkReaderStatisticsPtr->SessionCount.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "retry_count"_L, chunkReaderStatisticsPtr->RetryCount.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "pass_count"_L, chunkReaderStatisticsPtr->PassCount.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "block_count"_L, chunkReaderStatisticsPtr->BlockCount.load(std::memory_order::relaxed));
    jobStatistics->AddSample(prefixPath / "prefetched_block_count"_L, chunkReaderStatisticsPtr->PrefetchedBlockCount.load(std::memory_order::relaxed));
}

void DumpTimingStatistics(
    TStatistics* jobStatistics,
    const TStatisticPath& path,
    const TTimingStatistics& timingStatistics)
{
    jobStatistics->AddSample(path / "wait_time"_L, timingStatistics.WaitTime);
    jobStatistics->AddSample(path / "read_time"_L, timingStatistics.ReadTime);
    jobStatistics->AddSample(path / "idle_time"_L, timingStatistics.IdleTime);
}

////////////////////////////////////////////////////////////////////////////////

void LoadTimeHistogram(
    const IHistogramPtr& source,
    const TGaugeHistogram& timer)
{
    auto snapshot = source->GetSnapshot(true);
    const auto& bounds = snapshot.Bounds;

    for (int index = 0; index < std::ssize(bounds); ++index) {
        timer.Add(bounds[index], snapshot.Values[index]);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<double> GetWaitTimeHistogramBounds()
{
    return TChunkReaderStatistics::CreateRequestTimeHistogram()->GetSnapshot(false).Bounds;
}

////////////////////////////////////////////////////////////////////////////////

TChunkReaderStatisticsCounters::TChunkReaderStatisticsCounters(
    const NProfiling::TProfiler& profiler,
    const NProfiling::TProfiler& histogramProfiler)
    : DataBytesReadFromDisk_(profiler.Counter("/data_bytes_read_from_disk"))
    , DataIORequests_(profiler.Counter("/data_io_requests"))
    , DataBlocksReadFromDisk_(profiler.Counter("/data_blocks_read_from_disk"))
    , DataBytesTransmitted_(profiler.Counter("/data_bytes_transmitted"))
    , DataBytesReadFromCache_(profiler.Counter("/data_bytes_read_from_cache"))
    , WastedDataBytesReadFromDisk_(profiler.Counter("/wasted_data_bytes_read_from_disk"))
    , WastedDataBlocksReadFromDisk_(profiler.Counter("/wasted_data_blocks_read_from_disk"))
    , WastedDataBytesTransmitted_(profiler.Counter("/wasted_data_bytes_transmitted"))
    , WastedDataBytesReadFromCache_(profiler.Counter("/wasted_data_bytes_read_from_cache"))
    , MetaBytesReadFromDisk_(profiler.Counter("/meta_bytes_read_from_disk"))
    , MetaBytesTransmitted_(profiler.Counter("/meta_bytes_transmitted"))
    , MetaIORequests_(profiler.Counter("/meta_io_requests"))
    , WastedMetaBytesReadFromDisk_(profiler.Counter("/wasted_meta_bytes_read_from_disk"))
    , OmittedSuspiciousNodeCount_(profiler.Counter("/omitted_suspicious_node_count"))
    , P2PActivationCount_(profiler.Counter("/p2p_activation_count"))
    , RemoteCpuTime_(profiler.TimeCounter("/remote_cpu_time"))
    , DataWaitTime_(profiler.TimeCounter("/data_wait_time"))
    , MetaWaitTime_(profiler.TimeCounter("/meta_wait_time"))
    , MetaReadFromDiskTime_(profiler.TimeCounter("/meta_read_from_disk_time"))
    , PickPeerWaitTime_(profiler.TimeCounter("/pick_peer_wait_time"))
    , DataWaitTimeHistogram_(histogramProfiler.GaugeHistogram("/data_wait_time_histogram",
        GetWaitTimeHistogramBounds()))
    , MetaWaitTimeHistogram_(histogramProfiler.GaugeHistogram("/meta_wait_time_histogram",
        GetWaitTimeHistogramBounds()))
    , BlockCount_(profiler.Counter("/block_count"))
    , PrefetchedBlockCount_(profiler.Counter("/prefetched_block_count"))
{ }

void TChunkReaderStatisticsCounters::Increment(
    const TChunkReaderStatisticsPtr& chunkReaderStatistics,
    bool failed)
{
    DataBytesReadFromDisk_.Increment(chunkReaderStatistics->DataBytesReadFromDisk.load(std::memory_order::relaxed));
    DataIORequests_.Increment(chunkReaderStatistics->DataIORequests.load(std::memory_order::relaxed));
    DataBlocksReadFromDisk_.Increment(chunkReaderStatistics->DataBlocksReadFromDisk.load(std::memory_order::relaxed));
    DataBytesTransmitted_.Increment(chunkReaderStatistics->DataBytesTransmitted.load(std::memory_order::relaxed));
    DataBytesReadFromCache_.Increment(chunkReaderStatistics->DataBytesReadFromCache.load(std::memory_order::relaxed));
    WastedDataBytesReadFromDisk_.Increment(chunkReaderStatistics->WastedDataBytesReadFromDisk.load(std::memory_order::relaxed));
    WastedDataBlocksReadFromDisk_.Increment(chunkReaderStatistics->WastedDataBlocksReadFromDisk.load(std::memory_order::relaxed));

    MetaBytesTransmitted_.Increment(chunkReaderStatistics->MetaBytesTransmitted.load(std::memory_order::relaxed));
    MetaBytesReadFromDisk_.Increment(chunkReaderStatistics->MetaBytesReadFromDisk.load(std::memory_order::relaxed));
    MetaIORequests_.Increment(chunkReaderStatistics->MetaIORequests.load(std::memory_order::relaxed));
    OmittedSuspiciousNodeCount_.Increment(chunkReaderStatistics->OmittedSuspiciousNodeCount.load(std::memory_order::relaxed));

    P2PActivationCount_.Increment(chunkReaderStatistics->P2PActivationCount.load(std::memory_order::relaxed));

    RemoteCpuTime_.Add(TDuration::FromValue(chunkReaderStatistics->RemoteCpuTime.load(std::memory_order::relaxed)));

    DataWaitTime_.Add(TDuration::FromValue(chunkReaderStatistics->DataWaitTime.load(std::memory_order::relaxed)));
    MetaWaitTime_.Add(TDuration::FromValue(chunkReaderStatistics->MetaWaitTime.load(std::memory_order::relaxed)));
    MetaReadFromDiskTime_.Add(TDuration::FromValue(chunkReaderStatistics->MetaReadFromDiskTime.load(std::memory_order::relaxed)));
    PickPeerWaitTime_.Add(TDuration::FromValue(chunkReaderStatistics->PickPeerWaitTime.load(std::memory_order::relaxed)));

    if (failed) {
        WastedDataBytesReadFromDisk_.Increment(chunkReaderStatistics->DataBytesReadFromDisk.load(std::memory_order::relaxed));
        WastedDataBytesTransmitted_.Increment(chunkReaderStatistics->DataBytesTransmitted.load(std::memory_order::relaxed));
        WastedDataBytesReadFromCache_.Increment(chunkReaderStatistics->DataBytesReadFromCache.load(std::memory_order::relaxed));
        WastedMetaBytesReadFromDisk_.Increment(chunkReaderStatistics->MetaBytesReadFromDisk.load(std::memory_order::relaxed));
    }

    LoadTimeHistogram(chunkReaderStatistics->DataWaitTimeHistogram, DataWaitTimeHistogram_);
    LoadTimeHistogram(chunkReaderStatistics->MetaWaitTimeHistogram, MetaWaitTimeHistogram_);

    BlockCount_.Increment(chunkReaderStatistics->BlockCount.load(std::memory_order::relaxed));
    PrefetchedBlockCount_.Increment(chunkReaderStatistics->PrefetchedBlockCount.load(std::memory_order::relaxed));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
