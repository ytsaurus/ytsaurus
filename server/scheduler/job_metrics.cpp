#include "job_metrics.h"

#include <yt/server/scheduler/proto/controller_agent_tracker_service.pb.h>

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
    }
    metrics.TotalTime_ = FindNumericValue(statistics, "/time/total").Get(0);
    metrics.ExecTime_ = FindNumericValue(statistics, "/time/exec").Get(0);
    metrics.PrepareTime_ = FindNumericValue(statistics, "/time/prepare").Get(0);
    metrics.ArtifactsDownloadTime_ = FindNumericValue(statistics, "/time/artifacts_download").Get(0);

    metrics.AggregatedSmoothedCpuUsage_ = FindNumericValue(statistics, "/job_proxy/aggregated_smoothed_cpu_usage_x100").Get(0);
    metrics.AggregatedMaxCpuUsage_ = FindNumericValue(statistics, "/job_proxy/aggregated_max_cpu_usage_x100").Get(0);
    metrics.AggregatedPreemptableCpu_ = FindNumericValue(statistics, "/job_proxy/aggregated_preemptable_cpu_x100").Get(0);
    return metrics;
}

bool TJobMetrics::IsEmpty() const
{
    return DiskReads_ == 0 &&
        DiskWrites_ == 0 &&
        TimeCompleted_ == 0 &&
        TimeAborted_ == 0 &&
        TotalTime_ == 0 &&
        ExecTime_ == 0 &&
        PrepareTime_ == 0 &&
        ArtifactsDownloadTime_ == 0 &&
        AggregatedSmoothedCpuUsage_ == 0 &&
        AggregatedMaxCpuUsage_ == 0 &&
        AggregatedPreemptableCpu_ == 0;
}

void TJobMetrics::Profile(
    TProfileCollector& collector,
    const TString& prefix,
    const NProfiling::TTagIdList& tagIds) const
{
    //NB(renadeen): you cannot use EMetricType::Gauge here
    collector.Add(prefix + "/disk_reads", DiskReads_, EMetricType::Counter, tagIds);
    collector.Add(prefix + "/disk_writes", DiskWrites_, EMetricType::Counter, tagIds);
    collector.Add(prefix + "/time_aborted", TimeAborted_, EMetricType::Counter, tagIds);
    collector.Add(prefix + "/time_completed", TimeCompleted_, EMetricType::Counter, tagIds);
    collector.Add(prefix + "/total_time", TotalTime_, EMetricType::Counter, tagIds);
    collector.Add(prefix + "/exec_time", ExecTime_, EMetricType::Counter, tagIds);
    collector.Add(prefix + "/prepare_time", PrepareTime_, EMetricType::Counter, tagIds);
    collector.Add(prefix + "/artifacts_download_time", ArtifactsDownloadTime_, EMetricType::Counter, tagIds);
    collector.Add(prefix + "/aggregated_smoothed_cpu_usage_x100", AggregatedSmoothedCpuUsage_, EMetricType::Counter, tagIds);
    collector.Add(prefix + "/aggregated_max_cpu_usage_x100", AggregatedMaxCpuUsage_, EMetricType::Counter, tagIds);
    collector.Add(prefix + "/aggregated_preemptable_cpu_x100", AggregatedPreemptableCpu_, EMetricType::Counter, tagIds);
}

void TJobMetrics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, DiskReads_);
    Persist(context, DiskWrites_);
    Persist(context, TimeCompleted_);
    Persist(context, TimeAborted_);
    Persist(context, TotalTime_);
    Persist(context, ExecTime_);
    Persist(context, PrepareTime_);
    Persist(context, ArtifactsDownloadTime_);
    Persist(context, AggregatedSmoothedCpuUsage_);
    Persist(context, AggregatedMaxCpuUsage_);
    Persist(context, AggregatedPreemptableCpu_);
}

////////////////////////////////////////////////////////////////////////////////

TJobMetrics& operator+=(TJobMetrics& lhs, const TJobMetrics& rhs)
{
    lhs.DiskReads_ += rhs.DiskReads_;
    lhs.DiskWrites_ += rhs.DiskWrites_;
    lhs.TimeAborted_ += rhs.TimeAborted_;
    lhs.TimeCompleted_ += rhs.TimeCompleted_;
    lhs.TotalTime_ += rhs.TotalTime_;
    lhs.ExecTime_ += rhs.ExecTime_;
    lhs.PrepareTime_ += rhs.PrepareTime_;
    lhs.ArtifactsDownloadTime_ += rhs.ArtifactsDownloadTime_;
    lhs.AggregatedSmoothedCpuUsage_ += rhs.AggregatedSmoothedCpuUsage_;
    lhs.AggregatedMaxCpuUsage_ += rhs.AggregatedMaxCpuUsage_;
    lhs.AggregatedPreemptableCpu_ += rhs.AggregatedPreemptableCpu_;
    return lhs;
}

TJobMetrics& operator-=(TJobMetrics& lhs, const TJobMetrics& rhs)
{
    lhs.DiskReads_ -= rhs.DiskReads_;
    lhs.DiskWrites_ -= rhs.DiskWrites_;
    lhs.TimeAborted_ -= rhs.TimeAborted_;
    lhs.TimeCompleted_ -= rhs.TimeCompleted_;
    lhs.TotalTime_ -= rhs.TotalTime_;
    lhs.ExecTime_ -= rhs.ExecTime_;
    lhs.PrepareTime_ -= rhs.PrepareTime_;
    lhs.ArtifactsDownloadTime_ -= rhs.ArtifactsDownloadTime_;
    lhs.AggregatedSmoothedCpuUsage_ -= rhs.AggregatedSmoothedCpuUsage_;
    lhs.AggregatedMaxCpuUsage_ -= rhs.AggregatedMaxCpuUsage_;
    lhs.AggregatedPreemptableCpu_ -= rhs.AggregatedPreemptableCpu_;
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

void ToProto(NScheduler::NProto::TJobMetrics* protoJobMetrics, const NScheduler::TJobMetrics& jobMetrics)
{
    protoJobMetrics->set_disk_reads(jobMetrics.GetDiskReads());
    protoJobMetrics->set_disk_writes(jobMetrics.GetDiskWrites());
    protoJobMetrics->set_time_completed(jobMetrics.GetTimeCompleted());
    protoJobMetrics->set_time_aborted(jobMetrics.GetTimeAborted());
    protoJobMetrics->set_total_time(jobMetrics.GetTotalTime());
    protoJobMetrics->set_exec_time(jobMetrics.GetExecTime());
    protoJobMetrics->set_prepare_time(jobMetrics.GetPrepareTime());
    protoJobMetrics->set_artifacts_download_time(jobMetrics.GetArtifactsDownloadTime());
    protoJobMetrics->set_aggregated_smoothed_cpu_usage(jobMetrics.GetAggregatedSmoothedCpuUsage());
    protoJobMetrics->set_aggregated_max_cpu_usage(jobMetrics.GetAggregatedMaxCpuUsage());
    protoJobMetrics->set_aggregated_preemptable_cpu(jobMetrics.GetAggregatedPreemptableCpu());
}

void FromProto(NScheduler::TJobMetrics* jobMetrics, const NScheduler::NProto::TJobMetrics& protoJobMetrics)
{
    jobMetrics->SetDiskReads(protoJobMetrics.disk_reads());
    jobMetrics->SetDiskWrites(protoJobMetrics.disk_writes());
    jobMetrics->SetTimeCompleted(protoJobMetrics.time_completed());
    jobMetrics->SetTimeAborted(protoJobMetrics.time_aborted());
    jobMetrics->SetTotalTime(protoJobMetrics.total_time());
    jobMetrics->SetExecTime(protoJobMetrics.exec_time());
    jobMetrics->SetPrepareTime(protoJobMetrics.prepare_time());
    jobMetrics->SetArtifactsDownloadTime(protoJobMetrics.artifacts_download_time());
    jobMetrics->SetAggregatedSmoothedCpuUsage(protoJobMetrics.aggregated_smoothed_cpu_usage());
    jobMetrics->SetAggregatedMaxCpuUsage(protoJobMetrics.aggregated_max_cpu_usage());
    jobMetrics->SetAggregatedPreemptableCpu(protoJobMetrics.aggregated_preemptable_cpu());
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
