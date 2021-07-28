#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/proto/chunk_reader_statistics.pb.h>

#include <yt/yt/ytlib/job_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/timing_statistics.h>

#include <yt/yt/core/profiling/profiler.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderStatistics
    : public TRefCounted
{
    std::atomic<i64> DataBytesReadFromDisk{0};
    std::atomic<i64> DataBytesTransmitted{0};
    std::atomic<i64> DataBytesReadFromCache{0};
    std::atomic<i64> MetaBytesReadFromDisk{0};
    std::atomic<i64> OmittedSuspiciousNodeCount{0};

    // TODO(prime@): replace with max time. Cumulative disk IO time has not physical meaning.
    std::atomic<NProfiling::TValue> DataWaitTime{0};
    std::atomic<NProfiling::TValue> MetaWaitTime{0};
    std::atomic<NProfiling::TValue> MetaReadFromDiskTime{0};
    std::atomic<NProfiling::TValue> PickPeerWaitTime{0};
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
    TStatistics* jobStatisitcs,
    const TString& path,
    const TChunkReaderStatisticsPtr& chunkReaderStatisticsPtr);

void DumpTimingStatistics(
    TStatistics* jobStatistics,
    const TString& path,
    const NTableClient::TTimingStatistics& timingStatistics);

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderStatisticsCounters
{
public:
    TChunkReaderStatisticsCounters() = default;

    explicit TChunkReaderStatisticsCounters(const NProfiling::TProfiler& profiler);

    void Increment(const TChunkReaderStatisticsPtr& chunkReaderStatistics);

private:
    NProfiling::TCounter DataBytesReadFromDisk_;
    NProfiling::TCounter DataBytesTransmitted_;
    NProfiling::TCounter DataBytesReadFromCache_;
    NProfiling::TCounter MetaBytesReadFromDisk_;
    NProfiling::TCounter OmittedSuspiciousNodeCount_;

    NProfiling::TTimeCounter DataWaitTime_;
    NProfiling::TTimeCounter MetaWaitTime_;
    NProfiling::TTimeCounter MetaReadFromDiskTime_;
    NProfiling::TTimeCounter PickPeerWaitTime_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

