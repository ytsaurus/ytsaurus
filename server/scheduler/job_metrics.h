#pragma once

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/core/profiling/public.h>
#include <util/system/defaults.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJobMetrics
{
    friend TJobMetrics& operator+=(TJobMetrics& lhs, const TJobMetrics& rhs);
    friend TJobMetrics& operator-=(TJobMetrics& lhs, const TJobMetrics& rhs);
    friend TJobMetrics operator+(const TJobMetrics& lhs, const TJobMetrics& rhs);
    friend TJobMetrics operator-(const TJobMetrics& lhs, const TJobMetrics& rhs);

public:
    static TJobMetrics FromJobTrackerStatistics(const NJobTrackerClient::TStatistics& statistics);

    void SendToProfiler(
        const NProfiling::TProfiler& profiler,
        const TString& prefix,
        const NProfiling::TTagIdList& tagIds) const;

private:
    i64 DiskReads_ = 0;
    i64 DiskWrites_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TJobMetrics& operator+=(TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics& operator-=(TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics operator+(const TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics operator-(const TJobMetrics& lhs, const TJobMetrics& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
