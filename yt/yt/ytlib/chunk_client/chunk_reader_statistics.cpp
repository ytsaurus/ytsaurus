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

#define ITERATE_CHUNK_READER_STATISTICS_INTEGER_FIELDS(XX) \
    XX(DataBytesReadFromDisk, data_bytes_read_from_disk) \
    XX(DataBlocksReadFromDisk, data_blocks_read_from_disk) \
    XX(DataIORequests, data_io_requests) \
    XX(DataBytesTransmitted, data_bytes_transmitted) \
    XX(DataBytesReadFromCache, data_bytes_read_from_cache) \
    XX(WastedDataBytesReadFromDisk, wasted_data_bytes_read_from_disk) \
    XX(WastedDataBlocksReadFromDisk, wasted_data_blocks_read_from_disk) \
    XX(MetaBytesReadFromDisk, meta_bytes_read_from_disk) \
    XX(MetaBytesTransmitted, meta_bytes_transmitted) \
    XX(MetaIORequests, meta_io_requests) \
    XX(OmittedSuspiciousNodeCount, omitted_suspicious_node_count) \
    XX(P2PActivationCount, p2p_activation_count) \
    XX(RemoteCpuTime, remote_cpu_time) \
    XX(DataWaitTime, data_wait_time) \
    XX(MetaWaitTime, meta_wait_time) \
    XX(MetaReadFromDiskTime, meta_read_from_disk_time) \
    XX(PickPeerWaitTime, pick_peer_wait_time) \
    XX(SessionCount, session_count) \
    XX(RetryCount, retry_count) \
    XX(PassCount, pass_count) \
    XX(BlockCount, block_count) \
    XX(PrefetchedBlockCount, prefetched_block_count)


void TChunkReaderStatistics::AddFrom(const TChunkReaderStatisticsPtr& from)
{
    #define XX(fieldName, _protoName) \
        fieldName.fetch_add(from->fieldName.load(std::memory_order::relaxed), std::memory_order::relaxed);
    ITERATE_CHUNK_READER_STATISTICS_INTEGER_FIELDS(XX)
    #undef XX
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkReaderStatistics* protoChunkReaderStatistics, const TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    #define XX(fieldName, protoName) \
        protoChunkReaderStatistics->set_ ## protoName(chunkReaderStatistics->fieldName.load(std::memory_order::relaxed));
    ITERATE_CHUNK_READER_STATISTICS_INTEGER_FIELDS(XX)
    #undef XX
}

void FromProto(TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    chunkReaderStatistics = New<TChunkReaderStatistics>();
    #define XX(fieldName, protoName) \
        chunkReaderStatistics->fieldName.store(protoChunkReaderStatistics.protoName(), std::memory_order::relaxed);
    ITERATE_CHUNK_READER_STATISTICS_INTEGER_FIELDS(XX)
    #undef XX

}

void UpdateFromProto(const TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr, const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
{
    const auto& chunkReaderStatistics = *chunkReaderStatisticsPtr;
    #define XX(fieldName, protoName) \
        chunkReaderStatistics->fieldName.fetch_add(protoChunkReaderStatistics.protoName(), std::memory_order::relaxed);
    ITERATE_CHUNK_READER_STATISTICS_INTEGER_FIELDS(XX)
    #undef XX
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
    , DataBlocksReadFromDisk_(profiler.Counter("/data_blocks_read_from_disk"))
    , DataIORequests_(profiler.Counter("/data_io_requests"))
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
    DataBlocksReadFromDisk_.Increment(chunkReaderStatistics->DataBlocksReadFromDisk.load(std::memory_order::relaxed));
    DataIORequests_.Increment(chunkReaderStatistics->DataIORequests.load(std::memory_order::relaxed));
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
