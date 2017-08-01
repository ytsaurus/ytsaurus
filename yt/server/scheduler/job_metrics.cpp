#include "job_metrics.h"

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NScheduler {

using namespace NProfiling;
using namespace NJobTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TJobMetrics TJobMetrics::FromJobTrackerStatistics(const NJobTrackerClient::TStatistics& statistics, EJobState jobState)
{
    TJobMetrics metrics;
    metrics.DiskReads_ = FindNumericValue(statistics, "/user_job/block_io/io_read").Get(0);
    metrics.DiskWrites_ = FindNumericValue(statistics, "/user_job/block_io/io_write").Get(0);
    if (jobState == EJobState::Completed) {
        metrics.TimeCompleted_ = FindNumericValue(statistics, "/time/total").Get(0);
    } else if (jobState == EJobState::Aborted) {
        metrics.TimeAborted_ = FindNumericValue(statistics, "/time/total").Get(0);
    } else {
        // pass
    }
    return metrics;
}

void TJobMetrics::SendToProfiler(
    const NProfiling::TProfiler& profiler,
    const TString& prefix,
    const NProfiling::TTagIdList& tagIds) const
{
    profiler.Enqueue(prefix + "/disk_reads", DiskReads_, EMetricType::Counter, tagIds);
    profiler.Enqueue(prefix + "/disk_writes", DiskWrites_, EMetricType::Counter, tagIds);
    profiler.Enqueue(prefix + "/time_aborted", TimeAborted_, EMetricType::Counter, tagIds);
    profiler.Enqueue(prefix + "/time_completed", TimeCompleted_, EMetricType::Counter, tagIds);
}

////////////////////////////////////////////////////////////////////////////////

TJobMetrics& operator+=(TJobMetrics& lhs, const TJobMetrics& rhs)
{
    lhs.DiskReads_ += rhs.DiskReads_;
    lhs.DiskWrites_ += rhs.DiskWrites_;
    lhs.TimeAborted_ += rhs.TimeAborted_;
    lhs.TimeCompleted_ += rhs.TimeCompleted_;
    return lhs;
}

TJobMetrics& operator-=(TJobMetrics& lhs, const TJobMetrics& rhs)
{
    lhs.DiskReads_ -= rhs.DiskReads_;
    lhs.DiskWrites_ -= rhs.DiskWrites_;
    lhs.TimeAborted_ -= rhs.TimeAborted_;
    lhs.TimeCompleted_ -= rhs.TimeCompleted_;
    return lhs;
}

TJobMetrics operator-(const TJobMetrics& lhs, const TJobMetrics& rhs)
{
    TJobMetrics result = lhs;
    result -= rhs;
    return result;
}

TJobMetrics operator+(const TJobMetrics& lhs, const TJobMetrics& rhs)
{
    TJobMetrics result = lhs;
    result += rhs;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
