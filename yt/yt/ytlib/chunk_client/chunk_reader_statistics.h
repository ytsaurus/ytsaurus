#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/proto/chunk_reader_statistics.pb.h>

#include <yt/yt/ytlib/table_client/timing_statistics.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderStatistics
    : public TRefCounted
{
    std::atomic<i64> DataBytesReadFromDisk = 0;
    std::atomic<i64> DataIORequests = 0;
    std::atomic<i64> DataBytesTransmitted = 0;
    std::atomic<i64> DataBytesReadFromCache = 0;
    std::atomic<i64> MetaBytesReadFromDisk = 0;
    // COMPAT(babenko): drop
    std::atomic<i64> OmittedSuspiciousNodeCount = 0;

    // TODO(akozhikhov): Examine whether corresponding plot has non-zero values for dynamic tables.
    std::atomic<i64> P2PActivationCount = 0;

    std::atomic<NProfiling::TValue> RemoteCpuTime = 0;

    // TODO(akozhikhov): Replace with max time or with histogram with predefined percentiles.
    // Cumulative disk IO time has no physical meaning.
    std::atomic<NProfiling::TValue> DataWaitTime = 0;
    std::atomic<NProfiling::TValue> MetaWaitTime = 0;
    std::atomic<NProfiling::TValue> MetaReadFromDiskTime = 0;
    std::atomic<NProfiling::TValue> PickPeerWaitTime = 0;

    static constexpr TDuration MinTrackedLatency = TDuration::MicroSeconds(1);
    static constexpr TDuration MaxTrackedLatency = TDuration::Seconds(125);

    NProfiling::IHistogramImplPtr DataWaitTimeHistogram = CreateRequestTimeHistogram();
    NProfiling::IHistogramImplPtr MetaWaitTimeHistogram = CreateRequestTimeHistogram();

    static NProfiling::IHistogramImplPtr CreateRequestTimeHistogram();

    void RecordDataWaitTime(TDuration duration);
    void RecordMetaWaitTime(TDuration duration);
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
    TStatistics* jobStatistics,
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

    explicit TChunkReaderStatisticsCounters(
        const NProfiling::TProfiler& defaultProfiler,
        const NProfiling::TProfiler& histogramProfiler = {});

    void Increment(
        const TChunkReaderStatisticsPtr& chunkReaderStatistics,
        bool failed);

private:
    NProfiling::TCounter DataBytesReadFromDisk_;
    NProfiling::TCounter DataIORequests_;
    NProfiling::TCounter DataBytesTransmitted_;
    NProfiling::TCounter DataBytesReadFromCache_;
    NProfiling::TCounter WastedDataBytesReadFromDisk_;
    NProfiling::TCounter WastedDataBytesTransmitted_;
    NProfiling::TCounter WastedDataBytesReadFromCache_;

    NProfiling::TCounter MetaBytesReadFromDisk_;
    NProfiling::TCounter WastedMetaBytesReadFromDisk_;

    NProfiling::TCounter OmittedSuspiciousNodeCount_;

    NProfiling::TCounter P2PActivationCount_;

    NProfiling::TTimeCounter RemoteCpuTime_;

    NProfiling::TTimeCounter DataWaitTime_;
    NProfiling::TTimeCounter MetaWaitTime_;
    NProfiling::TTimeCounter MetaReadFromDiskTime_;
    NProfiling::TTimeCounter PickPeerWaitTime_;

    NProfiling::TGaugeHistogram DataWaitTimeHistogram_;
    NProfiling::TGaugeHistogram MetaWaitTimeHistogram_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
