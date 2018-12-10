#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_reader_statistics.pb.h>

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderStatistics
    : public TRefCounted
{
    std::atomic<i64> DataBytesReadFromDisk{0};
    std::atomic<i64> DataBytesReadFromCache{0};
    std::atomic<i64> MetaBytesReadFromDisk{0};
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderStatistics)

void ToProto(
    NProto::TChunkReaderStatistics* protoChunkReaderStatistics,
    const TChunkReaderStatisticsPtr& chunkReaderStatistics);
void FromProto(
    TChunkReaderStatisticsPtr chunkReaderStatistics,
    NProto::TChunkReaderStatistics* protoChunkReaderStatistics);

void UpdateFromProto(
    const TChunkReaderStatisticsPtr* chunkReaderStatisticsPtr,
    const NProto::TChunkReaderStatistics& protoChunkReaderStatistics);

void DumpChunkReaderStatistics(
    NJobTrackerClient::TStatistics* jobStatisitcs,
    const TString& path,
    const TChunkReaderStatisticsPtr& chunkReaderStatisticsPtr);

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderStatisticsCounters
{
public:
    explicit TChunkReaderStatisticsCounters(
        const NYPath::TYPath& path = NYPath::TYPath(),
        const NProfiling::TTagIdList& tagIds = NProfiling::EmptyTagIds);

    void Increment(
        const NProfiling::TProfiler& profiler,
        const TChunkReaderStatisticsPtr& chunkReaderStatistics);

private:
    NProfiling::TMonotonicCounter DataBytesReadFromDisk;
    NProfiling::TMonotonicCounter DataBytesReadFromCache;
    NProfiling::TMonotonicCounter MetaBytesReadFromDisk;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

