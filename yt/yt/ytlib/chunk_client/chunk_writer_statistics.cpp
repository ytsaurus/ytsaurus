#include "chunk_writer_statistics.h"

#include <yt/yt/core/misc/statistics.h>

#include <yt/yt/library/profiling/solomon/sensor.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkWriterStatistics* protoChunkWriterStatistics, const TChunkWriterStatisticsPtr& chunkWriterStatistics)
{
    protoChunkWriterStatistics->set_data_bytes_written_to_disk(chunkWriterStatistics->DataBytesWrittenToDisk.load(std::memory_order::relaxed));
    protoChunkWriterStatistics->set_data_io_write_requests(chunkWriterStatistics->DataIOWriteRequests.load(std::memory_order::relaxed));
    protoChunkWriterStatistics->set_meta_bytes_written_to_disk(chunkWriterStatistics->MetaBytesWrittenToDisk.load(std::memory_order::relaxed));
    protoChunkWriterStatistics->set_meta_io_write_requests(chunkWriterStatistics->MetaIOWriteRequests.load(std::memory_order::relaxed));
}

void FromProto(TChunkWriterStatisticsPtr* chunkWriterStatisticsPtr, const NProto::TChunkWriterStatistics& protoChunkWriterStatistics)
{
    auto& chunkWriterStatistics = *chunkWriterStatisticsPtr;
    chunkWriterStatistics = New<TChunkWriterStatistics>();
    chunkWriterStatistics->DataBytesWrittenToDisk.store(protoChunkWriterStatistics.data_bytes_written_to_disk(), std::memory_order::relaxed);
    chunkWriterStatistics->DataIOWriteRequests.store(protoChunkWriterStatistics.data_io_write_requests(), std::memory_order::relaxed);
    chunkWriterStatistics->MetaBytesWrittenToDisk.store(protoChunkWriterStatistics.meta_bytes_written_to_disk(), std::memory_order::relaxed);
    chunkWriterStatistics->MetaIOWriteRequests.store(protoChunkWriterStatistics.meta_io_write_requests(), std::memory_order::relaxed);
}

void UpdateFromProto(const TChunkWriterStatisticsPtr* chunkWriterStatisticsPtr, const NProto::TChunkWriterStatistics& protoChunkWriterStatistics)
{
    const auto& chunkWriterStatistics = *chunkWriterStatisticsPtr;
    chunkWriterStatistics->DataBytesWrittenToDisk.fetch_add(protoChunkWriterStatistics.data_bytes_written_to_disk(), std::memory_order::relaxed);
    chunkWriterStatistics->DataIOWriteRequests.fetch_add(protoChunkWriterStatistics.data_io_write_requests(), std::memory_order::relaxed);
    chunkWriterStatistics->MetaBytesWrittenToDisk.fetch_add(protoChunkWriterStatistics.meta_bytes_written_to_disk(), std::memory_order::relaxed);
    chunkWriterStatistics->MetaIOWriteRequests.fetch_add(protoChunkWriterStatistics.meta_io_write_requests(), std::memory_order::relaxed);
}

void DumpChunkWriterStatistics(
    TStatistics* jobStatistics,
    const TString& path,
    const TChunkWriterStatisticsPtr& chunkWriterStatisticsPtr)
{
    jobStatistics->AddSample(path + "/data_bytes_written_to_disk", chunkWriterStatisticsPtr->DataBytesWrittenToDisk.load(std::memory_order::relaxed));
    jobStatistics->AddSample(path + "/data_io_write_requests", chunkWriterStatisticsPtr->DataIOWriteRequests.load(std::memory_order::relaxed));
    jobStatistics->AddSample(path + "/meta_bytes_written_to_disk", chunkWriterStatisticsPtr->MetaBytesWrittenToDisk.load(std::memory_order::relaxed));
    jobStatistics->AddSample(path + "/meta_io_write_requests", chunkWriterStatisticsPtr->MetaIOWriteRequests.load(std::memory_order::relaxed));
}

////////////////////////////////////////////////////////////////////////////////

TChunkWriterStatisticsCounters::TChunkWriterStatisticsCounters(
    const NProfiling::TProfiler& profiler)
    : DataBytesWrittenToDisk_(profiler.Counter("/data_bytes_written_to_disk"))
    , DataIOWriteRequests_(profiler.Counter("/data_io_write_requests"))
    , MetaBytesWrittenToDisk_(profiler.Counter("/meta_bytes_written_to_disk"))
    , MetaIOWriteRequests_(profiler.Counter("/meta_io_write_requests"))
{ }

void TChunkWriterStatisticsCounters::Increment(
    const TChunkWriterStatisticsPtr& chunkWriterStatistics)
{
    DataBytesWrittenToDisk_.Increment(chunkWriterStatistics->DataBytesWrittenToDisk.load(std::memory_order::relaxed));
    DataIOWriteRequests_.Increment(chunkWriterStatistics->DataIOWriteRequests.load(std::memory_order::relaxed));
    MetaBytesWrittenToDisk_.Increment(chunkWriterStatistics->MetaBytesWrittenToDisk.load(std::memory_order::relaxed));
    MetaIOWriteRequests_.Increment(chunkWriterStatistics->MetaIOWriteRequests.load(std::memory_order::relaxed));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
