#pragma once

#include <yt/ytlib/job_tracker_client/statistics.h>
#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/profiling/public.h>

#include <yt/core/misc/phoenix.h>

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
    DEFINE_BYVAL_RW_PROPERTY(i64, DiskReads);
    DEFINE_BYVAL_RW_PROPERTY(i64, DiskWrites);
    DEFINE_BYVAL_RW_PROPERTY(i64, TimeCompleted);
    DEFINE_BYVAL_RW_PROPERTY(i64, TimeAborted);

public:
    static TJobMetrics FromJobTrackerStatistics(
        const NJobTrackerClient::TStatistics& statistics,
        NJobTrackerClient::EJobState jobState);

    void SendToProfiler(
        const NProfiling::TProfiler& profiler,
        const TString& prefix,
        const NProfiling::TTagIdList& tagIds) const;

    void Persist(const NPhoenix::TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

TJobMetrics& operator+=(TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics& operator-=(TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics operator+(const TJobMetrics& lhs, const TJobMetrics& rhs);
TJobMetrics operator-(const TJobMetrics& lhs, const TJobMetrics& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TOperationJobMetrics
{
    TOperationId OperationId;
    TJobMetrics JobMetrics;
};

namespace NProto {

void ToProto(NScheduler::NProto::TOperationJobMetrics* protoOperationJobMetrics, const NScheduler::TOperationJobMetrics& operationJobMetrics);
void FromProto(NScheduler::TOperationJobMetrics* operationJobMetrics, const NScheduler::NProto::TOperationJobMetrics& protoOperationJobMetrics);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
