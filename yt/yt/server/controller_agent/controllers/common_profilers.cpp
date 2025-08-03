#include "common_profilers.h"

#include "job_info.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

using namespace NProfiling;

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

TJobProfiler::TJobProfiler(IInvokerPtr profilerInvoker)
    : ProfilerInvoker_(std::move(profilerInvoker))
    , TotalCompletedJobTime_(ControllerAgentProfiler().TimeCounter("/jobs/total_completed_wall_time"))
    , TotalFailedJobTime_(ControllerAgentProfiler().TimeCounter("/jobs/total_failed_wall_time"))
    , TotalAbortedJobTime_(ControllerAgentProfiler().TimeCounter("/jobs/total_aborted_wall_time"))
{ }

void TJobProfiler::ProfileStartedJob(const TJoblet& joblet)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    auto jobType = joblet.JobType;
    auto treeId = joblet.TreeId;

    GetProfilerInvoker()->Invoke(
        BIND(&TJobProfiler::DoProfileStartedJob, MakeStrong(this), jobType, Passed(std::move(treeId))));
}

void TJobProfiler::DoProfileStartedJob(EJobType jobType, TString treeId)
{
    auto key = std::tuple(jobType, treeId);

    auto it = StartedJobCounters_.find(key);
    if (it == StartedJobCounters_.end()) {
        auto counter = ControllerAgentProfiler()
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/started_job_count");

        it = StartedJobCounters_.emplace(
            std::move(key),
            std::move(counter)).first;
    }

    it->second.Increment();

    DoUpdateInProgressJobCount(
        EJobState::Waiting,
        jobType,
        std::move(treeId),
        /*increment*/ true);
}

void TJobProfiler::ProfileRunningJob(const NControllers::TJoblet& joblet)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_VERIFY(joblet.IsStarted());

    if (*joblet.JobState == EJobState::Running) {
        return;
    }

    auto jobType = joblet.JobType;
    auto treeId = joblet.TreeId;

    GetProfilerInvoker()->Invoke(
        BIND(
            &TJobProfiler::DoProfileRunningJob,
            MakeStrong(this),
            jobType,
            Passed(std::move(treeId))));
}

void TJobProfiler::DoProfileRunningJob(EJobType jobType, TString treeId)
{
    DoUpdateInProgressJobCount(
        EJobState::Waiting,
        jobType,
        treeId,
        /*increment*/ false);
    DoUpdateInProgressJobCount(
        EJobState::Running,
        jobType,
        std::move(treeId),
        /*increment*/ true);
}

void TJobProfiler::ProfileRevivedJob(const NControllers::TJoblet& joblet)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!joblet.JobState) {
        return;
    }

    YT_VERIFY(*joblet.JobState <= EJobState::Running);

    auto jobType = joblet.JobType;
    auto treeId = joblet.TreeId;

    GetProfilerInvoker()->Invoke(
        BIND(
            &TJobProfiler::DoProfileRevivedJob,
            MakeStrong(this),
            jobType,
            Passed(std::move(treeId)),
            *joblet.JobState));
}

void TJobProfiler::DoProfileRevivedJob(EJobType jobType, TString treeId, EJobState jobState)
{
    DoUpdateInProgressJobCount(
        jobState,
        jobType,
        std::move(treeId),
        /*increment*/ true);
}

void TJobProfiler::ProfileCompletedJob(const TJoblet& joblet, const TCompletedJobSummary& jobSummary)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto interruptionReason = jobSummary.InterruptionReason;

    auto jobType = joblet.JobType;
    auto treeId = joblet.TreeId;
    auto duration = GetJobDuration(joblet);

    GetProfilerInvoker()->Invoke(BIND(
        &TJobProfiler::DoProfileCompletedJob,
        MakeStrong(this),
        jobType,
        interruptionReason,
        Passed(std::move(treeId)),
        duration,
        joblet.JobState));
}

void TJobProfiler::DoProfileCompletedJob(
    EJobType jobType,
    EInterruptionReason interruptionReason,
    TString treeId,
    TDuration duration,
    std::optional<EJobState> previousJobState)
{
    auto key = std::tuple(jobType, interruptionReason, treeId);

    auto it = CompletedJobCounters_.find(key);
    if (it == CompletedJobCounters_.end()) {
        auto counter = ControllerAgentProfiler()
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("interruption_reason", FormatEnum(interruptionReason))
            .WithTag(ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/completed_job_count");

        it = CompletedJobCounters_.emplace(
            std::move(key),
            std::move(counter)).first;
    }
    it->second.Increment();

    TotalCompletedJobTime_.Add(duration);

    DoProfileFinishedJob(jobType, previousJobState, treeId);
}

void TJobProfiler::ProfileFailedJob(const TJoblet& joblet, [[maybe_unused]] const TFailedJobSummary& jobSummary)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto jobType = joblet.JobType;
    auto treeId = joblet.TreeId;
    auto duration = GetJobDuration(joblet);

    GetProfilerInvoker()->Invoke(BIND(
        &TJobProfiler::DoProfileFailedJob,
        MakeStrong(this),
        jobType,
        Passed(std::move(treeId)),
        duration,
        joblet.JobState));
}

void TJobProfiler::DoProfileFailedJob(
    EJobType jobType,
    TString treeId,
    TDuration duration,
    std::optional<EJobState> previousJobState)
{
    auto key = std::tuple(jobType, treeId);

    auto it = FailedJobCounters_.find(key);
    if (it == FailedJobCounters_.end()) {
        auto counter = ControllerAgentProfiler()
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/failed_job_count");

        it = FailedJobCounters_.emplace(
            std::move(key),
            std::move(counter)).first;
    }
    it->second.Increment();

    TotalFailedJobTime_.Add(duration);

    DoProfileFinishedJob(jobType, previousJobState, treeId);
}

void TJobProfiler::ProfileAbortedJob(const TJoblet& joblet, const TAbortedJobSummary& jobSummary)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto abortReason = jobSummary.AbortReason;
    // Job error may be missing if the job summary is synthetic.
    auto error = jobSummary.Error.value_or(TError());

    auto jobType = joblet.JobType;
    auto treeId = joblet.TreeId;

    auto duration = GetJobDuration(joblet);

    GetProfilerInvoker()->Invoke(BIND(
        &TJobProfiler::DoProfileAbortedJob,
        MakeStrong(this),
        jobType,
        abortReason,
        Passed(std::move(treeId)),
        duration,
        Passed(std::move(error)),
        joblet.JobState));
}

void TJobProfiler::DoProfileAbortedJob(
    EJobType jobType,
    EAbortReason abortReason,
    TString treeId,
    TDuration duration,
    TError error,
    std::optional<EJobState> previousJobState)
{
    auto key = std::tuple(jobType, abortReason, treeId);

    auto it = AbortedJobCounters_.find(key);
    if (it == AbortedJobCounters_.end()) {
        auto counter = ControllerAgentProfiler()
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("abort_reason", FormatEnum(abortReason))
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/aborted_job_count");

        it = AbortedJobCounters_.emplace(
            key,
            std::move(counter)).first;
    }
    it->second.Increment();

    if (duration) {
        TotalAbortedJobTime_.Add(duration);
    }

    ProfileAbortedJobByError(treeId, jobType, error, NRpc::EErrorCode::TransportError);
    ProfileAbortedJobByError(treeId, jobType, error, NNet::EErrorCode::ResolveTimedOut);

    DoProfileFinishedJob(jobType, previousJobState, treeId);
}

template <class EErrorCodeType>
void TJobProfiler::ProfileAbortedJobByError(
    const TString& treeId,
    EJobType jobType,
    const TError& error,
    EErrorCodeType errorCode)
{
    if (!error.FindMatching(errorCode)) {
        return;
    }

    auto key = std::tuple(jobType, static_cast<int>(errorCode), treeId);
    auto it = AbortedJobByErrorCounters_.find(key);
    if (it == AbortedJobByErrorCounters_.end()) {
        it = AbortedJobByErrorCounters_.emplace(
            std::move(key),
            ControllerAgentProfiler()
                .WithTag("job_type", FormatEnum(jobType))
                .WithTag("job_error", FormatEnum(errorCode))
                .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
                .Counter("/jobs/aborted_job_count_by_error")).first;
    }
    it->second.Increment();
}

void TJobProfiler::DoUpdateInProgressJobCount(
    EJobState jobState,
    EJobType jobType,
    TString treeId,
    bool increment)
{
    YT_VERIFY(jobState <= EJobState::Running);

    auto key = std::tuple(jobState, jobType, treeId);

    auto createGauge = [&] {
        return ControllerAgentProfiler()
            .WithTags(TTagSet(TTagList{
                {NScheduler::ProfilingPoolTreeKey, treeId},
                {"job_type", FormatEnum(jobType)},
                {"state", FormatEnum(jobState)}}))
            .Gauge("/allocations/running_allocation_count");
    };

    auto it = InProgressJobCounters_.find(key);
    if (it == InProgressJobCounters_.end()) {
        it = InProgressJobCounters_.emplace(
            key,
            std::pair(
                0,
                createGauge())).first;
    }

    auto& [count, gauge] = it->second;
    if (increment) {
        ++count;
    } else {
        --count;
    }
    gauge.Update(count);
}

void TJobProfiler::DoProfileFinishedJob(
    EJobType jobType,
    std::optional<EJobState> previousJobState,
    const TString& treeId)
{
    if (!previousJobState) {
        return;
    }

    YT_VERIFY(*previousJobState <= EJobState::Running);

    DoUpdateInProgressJobCount(*previousJobState, jobType, treeId, /*increment*/ false);
}

const IInvokerPtr& TJobProfiler::GetProfilerInvoker() const
{
    return ProfilerInvoker_;
}

////////////////////////////////////////////////////////////////////////////////

TScheduleJobProfiler::TScheduleJobProfiler(IInvokerPtr profilerInvoker)
    : ProfilerInvoker_(std::move(profilerInvoker))
{ }

void TScheduleJobProfiler::ProfileScheduleJobFailure(
    const std::string& treeId,
    EJobType jobType,
    EScheduleFailReason failReason,
    bool isJobFirst)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    TTypedScheduleFailureKey key(treeId, jobType, failReason, isJobFirst);

    ProfilerInvoker_->Invoke(BIND(
        &TScheduleJobProfiler::DoProfileTypedScheduleJobFailure,
        MakeStrong(this),
        Passed(std::move(key))));
}

void TScheduleJobProfiler::ProfileScheduleJobFailure(const std::string& treeId, EScheduleFailReason failReason)
{
    TScheduleFailureKey key(treeId, failReason);

    ProfilerInvoker_->Invoke(BIND(
        &TScheduleJobProfiler::DoProfileScheduleJobFailure,
        MakeStrong(this),
        Passed(std::move(key))));
}

void TScheduleJobProfiler::ProfileScheduleJobSuccess(
    const std::string& treeId,
    EJobType jobType,
    bool isJobFirst)
{
    TScheduleSuccessKey key(treeId, jobType, isJobFirst);

    ProfilerInvoker_->Invoke(BIND(
        &TScheduleJobProfiler::DoProfileScheduleJobSuccess,
        MakeStrong(this),
        Passed(std::move(key))));
}

void TScheduleJobProfiler::DoProfileTypedScheduleJobFailure(TTypedScheduleFailureKey key)
{
    auto it = TypedFailureCounters_.find(key);
    if (it == end(TypedFailureCounters_)) {
        const auto& [treeId, jobType, failReason, isJobFirst] = key;

        auto counter = ControllerAgentProfiler()
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("fail_reason", FormatEnum(failReason))
            .WithTag("is_job_first", std::string(FormatBool(isJobFirst)))
            .Counter("/jobs/schedule_job_failure_count");

        it = TypedFailureCounters_.emplace(
            std::move(key),
            std::move(counter)).first;
    }

    it->second.Increment();
}

void TScheduleJobProfiler::DoProfileScheduleJobFailure(TScheduleFailureKey key)
{
    auto it = FailureCounters_.find(key);
    if (it == end(FailureCounters_)) {
        const auto& [treeId, failReason] = key;

        auto counter = ControllerAgentProfiler()
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .WithTag("fail_reason", FormatEnum(failReason))
            .Counter("/jobs/schedule_job_failure_count");

        it = FailureCounters_.emplace(
            std::move(key),
            std::move(counter)).first;
    }

    it->second.Increment();
}

void TScheduleJobProfiler::DoProfileScheduleJobSuccess(TScheduleSuccessKey key)
{
    auto it = SuccessCounters_.find(key);
    if (it == end(SuccessCounters_)) {
        const auto& [treeId, jobType, isJobFirst] = key;

        auto counter = ControllerAgentProfiler()
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("is_job_first", std::string(FormatBool(isJobFirst)))
            .Counter("/jobs/schedule_job_success_count");

        it = SuccessCounters_.emplace(
            std::move(key),
            std::move(counter)).first;
    }

    it->second.Increment();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
