#include "job_profiler.h"

#include <yt/yt/server/controller_agent/controllers/job_info.h>

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using NControllers::TJoblet;
using NScheduler::ProfilingPoolTreeKey;

////////////////////////////////////////////////////////////////////////////////

namespace {

TDuration GetJobDuration(const TJoblet& joblet)
{
    YT_VERIFY(joblet.StartTime);
    YT_VERIFY(joblet.FinishTime);
    return joblet.FinishTime - joblet.StartTime;
}

}

////////////////////////////////////////////////////////////////////////////////

TJobProfiler::TJobProfiler()
    : ProfilerQueue_(New<NConcurrency::TActionQueue>("JobProfiler"))
    , TotalCompletedJobTime_(ControllerAgentProfiler.TimeCounter("/jobs/total_completed_wall_time"))
    , TotalFailedJobTime_(ControllerAgentProfiler.TimeCounter("/jobs/total_failed_wall_time"))
    , TotalAbortedJobTime_(ControllerAgentProfiler.TimeCounter("/jobs/total_aborted_wall_time"))
{ }

void TJobProfiler::ProfileStartedJob(const TJoblet& joblet, [[maybe_unused]] const TStartedJobSummary& jobSummary)
{
    VERIFY_THREAD_AFFINITY_ANY();
    const auto jobType = joblet.JobType;
    const auto& treeId = joblet.TreeId;

    auto key = std::make_tuple(jobType, treeId);

    GetProfilerInvoker()->Invoke(
        BIND(&TJobProfiler::DoProfileStartedJob, MakeStrong(this), Passed(std::move(key))));
}

void TJobProfiler::DoProfileStartedJob(TStartedJobCounterKey key)
{
    auto it = StartedJobCounter_.find(key);
    if (it == StartedJobCounter_.end()) {
        const auto& [jobType, treeId] = key;
        auto counter = ControllerAgentProfiler
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/started_job_count");

        it = StartedJobCounter_.emplace(
            std::move(key),
            std::move(counter)).first;
    }

    it->second.Increment();
}

void TJobProfiler::ProfileCompletedJob(const TJoblet& joblet, const TCompletedJobSummary& jobSummary)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto interruptReason = jobSummary.InterruptReason;

    const auto jobType = joblet.JobType;
    const auto& treeId = joblet.TreeId;
    const auto duration = GetJobDuration(joblet);

    auto key = std::make_tuple(jobType, interruptReason, treeId);

    GetProfilerInvoker()->Invoke(
        BIND(&TJobProfiler::DoProfileCompletedJob, MakeStrong(this), Passed(std::move(key)), duration));
}

void TJobProfiler::DoProfileCompletedJob(TCompletedJobCounterKey key, const TDuration duration)
{
    auto it = CompletedJobCounter_.find(key);
    if (it == CompletedJobCounter_.end()) {
        const auto& [jobType, interruptReason, treeId] = key;
        auto counter = ControllerAgentProfiler
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("interrupt_reason", FormatEnum(interruptReason))
            .WithTag(ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/completed_job_count");

        it = CompletedJobCounter_.emplace(
            std::move(key),
            std::move(counter)).first;
    }
    it->second.Increment();

    TotalCompletedJobTime_.Add(duration);
}

void TJobProfiler::ProfileFailedJob(const TJoblet& joblet, [[maybe_unused]] const TFailedJobSummary& jobSummary)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto jobType = joblet.JobType;
    const auto& treeId = joblet.TreeId;
    const auto duration = GetJobDuration(joblet);

    auto key = std::make_tuple(jobType, treeId);

    GetProfilerInvoker()->Invoke(
        BIND(&TJobProfiler::DoProfileFailedJob, MakeStrong(this), Passed(std::move(key)), duration));
}

void TJobProfiler::DoProfileFailedJob(TFailedJobCounterKey key, const TDuration duration)
{
    auto it = FailedJobCounter_.find(key);
    if (it == FailedJobCounter_.end()) {
        const auto& [jobType, treeId] = key;
        auto counter = ControllerAgentProfiler
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/failed_job_count");

        it = FailedJobCounter_.emplace(
            std::move(key),
            std::move(counter)).first;
    }
    it->second.Increment();

    TotalFailedJobTime_.Add(duration);
}

void TJobProfiler::ProfileAbortedJob(const TJoblet& joblet, const TAbortedJobSummary& jobSummary)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto abortReason = jobSummary.AbortReason;
    // Job result may be missing if the job summary is synthetic.
    auto error = jobSummary.Result
        ? NYT::FromProto<TError>(jobSummary.GetJobResult().error())
        : TError();

    const auto jobType = joblet.JobType;
    const auto& treeId = joblet.TreeId;

    const auto duration = GetJobDuration(joblet);

    auto key = std::make_tuple(jobType, abortReason, treeId);

    GetProfilerInvoker()->Invoke(
        BIND(&TJobProfiler::DoProfileAbortedJob, MakeStrong(this), Passed(std::move(key)), duration, Passed(std::move(error))));
}

void TJobProfiler::DoProfileAbortedJob(TAbortedJobCounterKey key, const TDuration duration, const TError error)
{
    const auto& [jobType, abortReason, treeId] = key;

    auto it = AbortedJobCounter_.find(key);
    if (it == AbortedJobCounter_.end()) {
        auto counter = ControllerAgentProfiler
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("abort_reason", FormatEnum(abortReason))
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/aborted_job_count");

        it = AbortedJobCounter_.emplace(
            key,
            std::move(counter)).first;
    }
    it->second.Increment();

    if (duration) {
        TotalAbortedJobTime_.Add(duration);
    }

    ProfileAbortedJobByError(treeId, jobType, error, NRpc::EErrorCode::TransportError);
    ProfileAbortedJobByError(treeId, jobType, error, NNet::EErrorCode::ResolveTimedOut);
}

template <class EErrorCodeType>
void TJobProfiler::ProfileAbortedJobByError(
    const TString& treeId,
    const EJobType jobType,
    const TError& error,
    const EErrorCodeType errorCode)
{
    if (!error.FindMatching(errorCode)) {
        return;
    }

    auto key = std::make_tuple(jobType, static_cast<int>(errorCode), treeId);
    auto it = AbortedJobByErrorCounter_.find(key);
    if (it == AbortedJobByErrorCounter_.end()) {
        it = AbortedJobByErrorCounter_.emplace(
            std::move(key),
            ControllerAgentProfiler
                .WithTag("job_type", FormatEnum(jobType))
                .WithTag("job_error", FormatEnum(errorCode))
                .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
                .Counter("/jobs/aborted_job_count_by_error")).first;
    }
    it->second.Increment();
}

IInvokerPtr TJobProfiler::GetProfilerInvoker() const
{
    return ProfilerQueue_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
