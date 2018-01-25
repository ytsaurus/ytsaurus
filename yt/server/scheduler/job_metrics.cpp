#include "job_metrics.h"

#include <yt/ytlib/scheduler/public.h>
#include <yt/ytlib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/core/profiling/profiler.h>

#include <yt/core/misc/protobuf_helpers.h>

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

namespace NProto
{

void ToProto(NScheduler::NProto::TJobMetrics* protoJobMetrics, const NScheduler::TJobMetrics& jobMetrics)
{
    protoJobMetrics->set_disk_reads(jobMetrics.GetDiskReads());
    protoJobMetrics->set_disk_writes(jobMetrics.GetDiskWrites());
    protoJobMetrics->set_time_completed(jobMetrics.GetTimeCompleted());
    protoJobMetrics->set_time_aborted(jobMetrics.GetTimeAborted());
}

void FromProto(NScheduler::TJobMetrics* jobMetrics, const NScheduler::NProto::TJobMetrics& protoJobMetrics)
{
    jobMetrics->SetDiskReads(protoJobMetrics.disk_reads());
    jobMetrics->SetDiskWrites(protoJobMetrics.disk_writes());
    jobMetrics->SetTimeCompleted(protoJobMetrics.time_completed());
    jobMetrics->SetTimeAborted(protoJobMetrics.time_aborted());
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

namespace NProto
{

void ToProto(
    NScheduler::NProto::TTreeTaggedJobMetrics* protoJobMetrics,
    const NScheduler::TTreeTaggedJobMetrics& jobMetrics)
{
    protoJobMetrics->set_tree_id(jobMetrics.TreeId);
    ToProto(protoJobMetrics->mutable_metrics(), jobMetrics.Metrics);
}

void FromProto(
    NScheduler::TTreeTaggedJobMetrics* jobMetrics,
    const NScheduler::NProto::TTreeTaggedJobMetrics& protoJobMetrics)
{
    jobMetrics->TreeId = protoJobMetrics.tree_id();
    FromProto(&jobMetrics->Metrics, protoJobMetrics.metrics());
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(
    NScheduler::NProto::TOperationJobMetrics* protoOperationJobMetrics,
    const NScheduler::TOperationJobMetrics& operationJobMetrics)
{
    using NYT::ToProto;

    ToProto(protoOperationJobMetrics->mutable_operation_id(), operationJobMetrics.OperationId);
    ToProto(protoOperationJobMetrics->mutable_metrics(), operationJobMetrics.Metrics);
}

void FromProto(
    NScheduler::TOperationJobMetrics* operationJobMetrics,
    const NScheduler::NProto::TOperationJobMetrics& protoOperationJobMetrics)
{
    using NYT::FromProto;

    FromProto(&operationJobMetrics->OperationId, protoOperationJobMetrics.operation_id());
    FromProto(&operationJobMetrics->Metrics, protoOperationJobMetrics.metrics());
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
