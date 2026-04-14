#include "common_profilers.h"

#include "job_info.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/net/public.h>

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TJobProfiler::ProfileStartedJob(const TJoblet& joblet)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto jobType = joblet.JobType;
    const auto& treeId = joblet.TreeId;

    auto key = std::tuple(jobType, treeId);
    auto [counter, inserted] = StartedJobCounters_.FindOrInsert(key, [&] {
        return ControllerAgentProfiler()
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/started_job_count");
    });
    counter->Increment();

    UpdateInProgressJobCount(EJobState::Waiting, jobType, treeId, /*increment*/ true);
}

void TJobProfiler::ProfileRunningJob(const NControllers::TJoblet& joblet)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_VERIFY(joblet.IsStarted());

    if (*joblet.JobState == EJobState::Running) {
        return;
    }

    auto jobType = joblet.JobType;
    const auto& treeId = joblet.TreeId;

    UpdateInProgressJobCount(EJobState::Waiting, jobType, treeId, /*increment*/ false);
    UpdateInProgressJobCount(EJobState::Running, jobType, treeId, /*increment*/ true);
}

void TJobProfiler::ProfileRevivedJob(const NControllers::TJoblet& joblet)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!joblet.JobState) {
        return;
    }

    YT_VERIFY(*joblet.JobState <= EJobState::Running);

    UpdateInProgressJobCount(*joblet.JobState, joblet.JobType, joblet.TreeId, /*increment*/ true);
}

void TJobProfiler::ProfileCompletedJob(const TJoblet& joblet, const TCompletedJobSummary& jobSummary)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto jobType = joblet.JobType;
    const auto& treeId = joblet.TreeId;
    auto interruptionReason = jobSummary.InterruptionReason;
    auto duration = GetJobDuration(joblet);

    auto key = std::tuple(jobType, interruptionReason, treeId);
    auto [counter, inserted] = CompletedJobCounters_.FindOrInsert(key, [&] {
        return ControllerAgentProfiler()
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("interruption_reason", FormatEnum(interruptionReason))
            .WithTag(ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/completed_job_count");
    });
    counter->Increment();

    TotalCompletedJobTime_.Add(duration);

    ProfileFinishedJob(jobType, joblet.JobState, treeId);
}

void TJobProfiler::ProfileFailedJob(const TJoblet& joblet, [[maybe_unused]] const TFailedJobSummary& jobSummary)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto jobType = joblet.JobType;
    const auto& treeId = joblet.TreeId;
    auto duration = GetJobDuration(joblet);

    auto key = std::tuple(jobType, treeId);
    auto [counter, inserted] = FailedJobCounters_.FindOrInsert(key, [&] {
        return ControllerAgentProfiler()
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/failed_job_count");
    });
    counter->Increment();

    TotalFailedJobTime_.Add(duration);

    ProfileFinishedJob(jobType, joblet.JobState, treeId);
}

void TJobProfiler::ProfileAbortedJob(const TJoblet& joblet, const TAbortedJobSummary& jobSummary)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto jobType = joblet.JobType;
    const auto& treeId = joblet.TreeId;
    auto abortReason = jobSummary.AbortReason;
    // Job error may be missing if the job summary is synthetic.
    auto error = jobSummary.Error.value_or(TError());
    auto duration = GetJobDuration(joblet);

    auto key = std::tuple(jobType, abortReason, treeId);
    auto [counter, inserted] = AbortedJobCounters_.FindOrInsert(key, [&] {
        return ControllerAgentProfiler()
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("abort_reason", FormatEnum(abortReason))
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/aborted_job_count");
    });
    counter->Increment();

    if (duration) {
        TotalAbortedJobTime_.Add(duration);
    }

    ProfileAbortedJobByError(treeId, jobType, error, NRpc::EErrorCode::TransportError);
    ProfileAbortedJobByError(treeId, jobType, error, NNet::EErrorCode::ResolveTimedOut);

    ProfileFinishedJob(jobType, joblet.JobState, treeId);
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
    auto [counter, inserted] = AbortedJobByErrorCounters_.FindOrInsert(key, [&] {
        return ControllerAgentProfiler()
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("job_error", FormatEnum(errorCode))
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .Counter("/jobs/aborted_job_count_by_error");
    });
    counter->Increment();
}

void TJobProfiler::UpdateInProgressJobCount(
    EJobState jobState,
    EJobType jobType,
    const TString& treeId,
    bool increment)
{
    YT_VERIFY(jobState <= EJobState::Running);

    auto key = std::tuple(jobState, jobType, treeId);

    auto [entryPtr, inserted] = InProgressJobCounters_.FindOrInsert(key, [&] {
        auto entry = New<TInProgressJobCounter>();
        entry->Gauge = ControllerAgentProfiler()
            .WithTags(TTagSet(TTagList{
                {NScheduler::ProfilingPoolTreeKey, treeId},
                {"job_type", FormatEnum(jobType)},
                {"state", FormatEnum(jobState)}}))
            .Gauge("/allocations/running_allocation_count");
        return entry;
    });

    auto& entry = **entryPtr;

    auto guard = Guard(entry.Lock);
    auto count = increment ? ++entry.Count : --entry.Count;
    entry.Gauge.Update(count);
}

void TJobProfiler::ProfileFinishedJob(
    EJobType jobType,
    std::optional<EJobState> previousJobState,
    const TString& treeId)
{
    if (!previousJobState) {
        return;
    }

    YT_VERIFY(*previousJobState <= EJobState::Running);

    UpdateInProgressJobCount(*previousJobState, jobType, treeId, /*increment*/ false);
}

////////////////////////////////////////////////////////////////////////////////

void TScheduleJobProfiler::ProfileScheduleJobFailure(
    const std::string& treeId,
    EJobType jobType,
    EScheduleFailReason failReason,
    bool isJobFirst)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    TTypedScheduleFailureKey key(treeId, jobType, failReason, isJobFirst);

    auto [counter, inserted] = TypedFailureCounters_.FindOrInsert(key, [&] {
        const auto& [treeId, jobType, failReason, isJobFirst] = key;
        return ControllerAgentProfiler()
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("fail_reason", FormatEnum(failReason))
            .WithTag("is_job_first", std::string(FormatBool(isJobFirst)))
            .Counter("/jobs/schedule_job_failure_count");
    });

    counter->Increment();
}

void TScheduleJobProfiler::ProfileScheduleJobFailure(const std::string& treeId, EScheduleFailReason failReason)
{
    TScheduleFailureKey key(treeId, failReason);

    auto [counter, inserted] = FailureCounters_.FindOrInsert(key, [&] {
        const auto& [treeId, failReason] = key;
        return ControllerAgentProfiler()
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .WithTag("fail_reason", FormatEnum(failReason))
            .Counter("/jobs/schedule_job_failure_count");
    });

    counter->Increment();
}

void TScheduleJobProfiler::ProfileScheduleJobSuccess(
    const std::string& treeId,
    EJobType jobType,
    bool isJobFirst,
    bool isLocal)
{
    TScheduleSuccessKey key(treeId, jobType, isJobFirst, isLocal);

    auto [counter, inserted] = SuccessCounters_.FindOrInsert(key, [&] {
        const auto& [treeId, jobType, isJobFirst, isLocal] = key;
        return ControllerAgentProfiler()
            .WithTag(NScheduler::ProfilingPoolTreeKey, treeId)
            .WithTag("job_type", FormatEnum(jobType))
            .WithTag("is_job_first", std::string(FormatBool(isJobFirst)))
            .WithTag("is_local", std::string(FormatBool(isLocal)))
            .Counter("/jobs/schedule_job_success_count");
    });

    counter->Increment();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
