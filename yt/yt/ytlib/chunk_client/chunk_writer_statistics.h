#pragma once

#include "public.h"

#include <yt/yt/core/misc/statistic_path.h>
#include <yt/yt/core/profiling/public.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_writer_statistics.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriterStatistics
    : public TRefCounted
{
    std::atomic<i64> DataBytesWrittenToDisk = 0;
    std::atomic<i64> DataIOWriteRequests = 0;
    std::atomic<i64> DataIOSyncRequests = 0;
    std::atomic<i64> MetaBytesWrittenToDisk = 0;
    std::atomic<i64> MetaIOWriteRequests = 0;
    std::atomic<i64> MetaIOSyncRequests = 0;
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterStatistics)

void ToProto(
    NProto::TChunkWriterStatistics* protoChunkWriterStatistics,
    const TChunkWriterStatisticsPtr& chunkWriterStatistics);
void FromProto(
    TChunkWriterStatisticsPtr chunkWriterStatistics,
    NProto::TChunkWriterStatistics* protoChunkWriterStatistics);

void UpdateFromProto(
    const TChunkWriterStatisticsPtr* chunkWriterStatisticsPtr,
    const NProto::TChunkWriterStatistics& protoChunkWriterStatistics);

void DumpChunkWriterStatistics(
    TStatistics* jobStatistics,
    const TString& path,
    const TChunkWriterStatisticsPtr& chunkWriterStatisticsPtr);

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterStatisticsCounters
{
public:
    TChunkWriterStatisticsCounters() = default;

    explicit TChunkWriterStatisticsCounters(
        const NProfiling::TProfiler& defaultProfiler);

    void Increment(
        const TChunkWriterStatisticsPtr& chunkWriterStatistics);

private:
    NProfiling::TCounter DataBytesWrittenToDisk_;
    NProfiling::TCounter DataIOWriteRequests_;
    NProfiling::TCounter DataIOSyncRequests_;

    NProfiling::TCounter MetaBytesWrittenToDisk_;
    NProfiling::TCounter MetaIOWriteRequests_;
    NProfiling::TCounter MetaIOSyncRequests_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
