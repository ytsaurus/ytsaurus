#include "job_metrics.h"

#include <yt/ytlib/scheduler/public.h>
#include <yt/ytlib/scheduler/proto/controller_agent_service.pb.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NScheduler {

using namespace NProfiling;
using namespace NJobTrackerClient;
using namespace NPhoenix;

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

bool TJobMetrics::IsEmpty() const
{
    return DiskReads_ == 0 &&
        DiskWrites_ == 0 &&
        TimeCompleted_ == 0 &&
        TimeAborted_ == 0;
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

void TJobMetrics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, DiskReads_);
    Persist(context, DiskWrites_);
    Persist(context, TimeCompleted_);
    Persist(context, TimeAborted_);
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

namespace NProto {

void ToProto(NScheduler::NProto::TOperationJobMetrics* protoOperationJobMetrics, const NScheduler::TOperationJobMetrics& operationJobMetrics)
{
    ToProto(protoOperationJobMetrics->mutable_operation_id(), operationJobMetrics.OperationId);
    protoOperationJobMetrics->set_disk_reads(operationJobMetrics.JobMetrics.GetDiskReads());
    protoOperationJobMetrics->set_disk_writes(operationJobMetrics.JobMetrics.GetDiskWrites());
    protoOperationJobMetrics->set_time_completed(operationJobMetrics.JobMetrics.GetTimeCompleted());
    protoOperationJobMetrics->set_time_aborted(operationJobMetrics.JobMetrics.GetTimeAborted());
}

void FromProto(NScheduler::TOperationJobMetrics* operationJobMetrics, const NScheduler::NProto::TOperationJobMetrics& protoOperationJobMetrics)
{
    FromProto(&operationJobMetrics->OperationId, protoOperationJobMetrics.operation_id());
    operationJobMetrics->JobMetrics.SetDiskReads(protoOperationJobMetrics.disk_reads());
    operationJobMetrics->JobMetrics.SetDiskWrites(protoOperationJobMetrics.disk_writes());
    operationJobMetrics->JobMetrics.SetTimeCompleted(protoOperationJobMetrics.time_completed());
    operationJobMetrics->JobMetrics.SetTimeAborted(protoOperationJobMetrics.time_aborted());
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
