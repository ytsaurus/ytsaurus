#include "public.h"
#include "structs.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent {

using namespace NScheduler;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

void MergeJobSummaries(
    TJobSummary& schedulerJobSummary,
    TJobSummary&& nodeJobSummary)
{
    YT_VERIFY(schedulerJobSummary.Id == nodeJobSummary.Id);

    schedulerJobSummary.StatisticsYson = std::move(nodeJobSummary.StatisticsYson);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStartedJobSummary::TStartedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : Id(FromProto<TJobId>(event->status().job_id()))
    , StartTime(FromProto<TInstant>(event->start_time()))
{
    YT_VERIFY(event->has_start_time());
}

////////////////////////////////////////////////////////////////////////////////

TJobSummary::TJobSummary(TJobId id, EJobState state)
    : Result()
    , Id(id)
    , State(state)
    , LogAndProfile(false)
{ }

TJobSummary::TJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : Id(FromProto<TJobId>(event->status().job_id()))
    , State(CheckedEnumCast<EJobState>(event->status().state()))
    , FinishTime(event->has_finish_time() ? std::make_optional(FromProto<TInstant>(event->finish_time())) : std::nullopt)
    , LogAndProfile(event->log_and_profile())
{
    auto* status = event->mutable_status();
    Result.Swap(status->mutable_result());
    TimeStatistics = FromProto<NJobAgent::TTimeStatistics>(status->time_statistics());
    if (status->has_statistics()) {
        StatisticsYson = TYsonString(status->statistics());
    }
    if (status->has_phase()) {
        Phase = static_cast<EJobPhase>(status->phase());
    }
    if (status->has_job_type()) {
        Type = static_cast<EJobType>(status->job_type());
    }

    if (status->has_status_timestamp()) {
        LastStatusUpdateTime = FromProto<TInstant>(status->status_timestamp());
    }

    JobExecutionCompleted = status->job_execution_completed();
}

TJobSummary::TJobSummary(NJobTrackerClient::NProto::TJobStatus* status)
    : Id(FromProto<TJobId>(status->job_id()))
    , State(CheckedEnumCast<EJobState>(status->state()))
{
    Result.Swap(status->mutable_result());
    TimeStatistics = FromProto<NJobAgent::TTimeStatistics>(status->time_statistics());
    if (status->has_statistics()) {
        StatisticsYson = TYsonString(status->statistics());
    }
    if (status->has_phase()) {
        Phase = CheckedEnumCast<EJobPhase>(status->phase());
    }

    LastStatusUpdateTime = FromProto<TInstant>(status->status_timestamp());
    JobExecutionCompleted = status->job_execution_completed();
}

void TJobSummary::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Result);
    Persist(context, Id);
    Persist(context, State);
    Persist(context, FinishTime);
    Persist(context, Statistics);
    Persist(context, StatisticsYson);
    Persist(context, LogAndProfile);
    Persist(context, ReleaseFlags);
    Persist(context, Phase);
    Persist(context, TimeStatistics);
}

////////////////////////////////////////////////////////////////////////////////

TCompletedJobSummary::TCompletedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : TJobSummary(event)
    , Abandoned(event->abandoned())
    , InterruptReason(static_cast<EInterruptReason>(event->interrupt_reason()))
{
    YT_VERIFY(event->has_abandoned());
    YT_VERIFY(event->has_interrupt_reason());
    const auto& schedulerResultExt = Result.GetExtension(NScheduler::NProto::TSchedulerJobResultExt::scheduler_job_result_ext);
    YT_VERIFY(
        (InterruptReason == EInterruptReason::None && schedulerResultExt.unread_chunk_specs_size() == 0) ||
        (InterruptReason != EInterruptReason::None && (
            schedulerResultExt.unread_chunk_specs_size() != 0 ||
            schedulerResultExt.restart_needed())));
    YT_VERIFY(State == ExpectedState);
}

TCompletedJobSummary::TCompletedJobSummary(NJobTrackerClient::NProto::TJobStatus* status)
    : TJobSummary(status)
{
    const auto& schedulerResultExt = Result.GetExtension(NScheduler::NProto::TSchedulerJobResultExt::scheduler_job_result_ext);
    YT_VERIFY(
        (InterruptReason == EInterruptReason::None && schedulerResultExt.unread_chunk_specs_size() == 0) ||
        (InterruptReason != EInterruptReason::None && (
            schedulerResultExt.unread_chunk_specs_size() != 0 ||
            schedulerResultExt.restart_needed())));
    YT_VERIFY(State == ExpectedState);
}

void TCompletedJobSummary::Persist(const TPersistenceContext& context)
{
    TJobSummary::Persist(context);

    using NYT::Persist;

    Persist(context, Abandoned);
    Persist(context, InterruptReason);
    // TODO(max42): now we persist only those completed job summaries that correspond
    // to non-interrupted jobs, because Persist(context, UnreadInputDataSlices) produces
    // lots of ugly template resolution errors. I wasn't able to fix it :(
    YT_VERIFY(InterruptReason == EInterruptReason::None);
    Persist(context, SplitJobCount);
}

////////////////////////////////////////////////////////////////////////////////

TAbortedJobSummary::TAbortedJobSummary(TJobId id, EAbortReason abortReason)
    : TJobSummary(id, EJobState::Aborted)
    , AbortReason(abortReason)
{ }

TAbortedJobSummary::TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason)
    : TJobSummary(other)
    , AbortReason(abortReason)
{ }

TAbortedJobSummary::TAbortedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : TJobSummary(event)
    , AbortReason(static_cast<EAbortReason>(event->abort_reason()))
{
    if (event->has_preempted_for()) {
        PreemptedFor = FromProto<NScheduler::TPreemptedFor>(event->preempted_for());
    }
    if (event->has_aborted_by_scheduler()) {
        AbortedByScheduler = event->aborted_by_scheduler();
    }

    YT_VERIFY(State == ExpectedState);
}

TAbortedJobSummary::TAbortedJobSummary(NJobTrackerClient::NProto::TJobStatus* status)
    : TJobSummary(status)
{
    YT_VERIFY(State == ExpectedState);
}

////////////////////////////////////////////////////////////////////////////////

TFailedJobSummary::TFailedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : TJobSummary(event)
{
    YT_VERIFY(State == ExpectedState);
}

TFailedJobSummary::TFailedJobSummary(NJobTrackerClient::NProto::TJobStatus* status)
    : TJobSummary(status)
{
    YT_VERIFY(State == ExpectedState);
}

////////////////////////////////////////////////////////////////////////////////

TRunningJobSummary::TRunningJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : TJobSummary(event)
    , Progress(event->status().progress())
    , StderrSize(event->status().stderr_size())
{ }

TRunningJobSummary::TRunningJobSummary(NJobTrackerClient::NProto::TJobStatus* status)
    : TJobSummary(status)
    , Progress(status->progress())
    , StderrSize(status->stderr_size())
{ }

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TFailedJobSummary> MergeJobSummaries(
    std::unique_ptr<TFailedJobSummary> schedulerJobSummary,
    std::unique_ptr<TFailedJobSummary> nodeJobSummary)
{
    MergeJobSummaries(*schedulerJobSummary, std::move(*nodeJobSummary));
    return schedulerJobSummary;
}

std::unique_ptr<TAbortedJobSummary> MergeJobSummaries(
    std::unique_ptr<TAbortedJobSummary> schedulerJobSummary,
    std::unique_ptr<TAbortedJobSummary> nodeJobSummary)
{
    MergeJobSummaries(*schedulerJobSummary, std::move(*nodeJobSummary));
    return schedulerJobSummary;
}

std::unique_ptr<TCompletedJobSummary> MergeJobSummaries(
    std::unique_ptr<TCompletedJobSummary> schedulerJobSummary,
    std::unique_ptr<TCompletedJobSummary> nodeJobSummary)
{
    MergeJobSummaries(*schedulerJobSummary, std::move(*nodeJobSummary));
    return schedulerJobSummary;
}

std::unique_ptr<TJobSummary> MergeSchedulerAndNodeFinishedJobSummaries(
    std::unique_ptr<TJobSummary> schedulerJobSummary,
    std::unique_ptr<TJobSummary> nodeJobSummary)
{
    switch (schedulerJobSummary->State) {
        case EJobState::Aborted:
            return MergeJobSummaries(
                SummaryCast<TAbortedJobSummary>(std::move(schedulerJobSummary)),
                SummaryCast<TAbortedJobSummary>(std::move(nodeJobSummary)));
        case EJobState::Completed:
            return MergeJobSummaries(
                SummaryCast<TCompletedJobSummary>(std::move(schedulerJobSummary)),
                SummaryCast<TCompletedJobSummary>(std::move(nodeJobSummary)));
        case EJobState::Failed:
            return MergeJobSummaries(
                SummaryCast<TFailedJobSummary>(std::move(schedulerJobSummary)),
                SummaryCast<TFailedJobSummary>(std::move(nodeJobSummary)));
        default:
            YT_ABORT();
    }
}

std::unique_ptr<TJobSummary> ParseJobSummary(NJobTrackerClient::NProto::TJobStatus* const status, const NLogging::TLogger& Logger)
{
    const auto state = static_cast<EJobState>(status->state());
    switch (state) {
        case EJobState::Completed:
            return std::make_unique<TCompletedJobSummary>(status);
        case EJobState::Failed:
            return std::make_unique<TFailedJobSummary>(status);
        case EJobState::Aborted:
            return std::make_unique<TAbortedJobSummary>(status);
        case EJobState::Running:
            return std::make_unique<TRunningJobSummary>(status);
        default:
            YT_LOG_ERROR(
                "Unexpected job state in parsing status (JobState: %v, JobId: %v)",
                state,
                FromProto<TJobId>(status->job_id()));
            YT_ABORT();
    }
}

bool ExpectsJobInfoFromNode(const TJobSummary& jobSummary) noexcept
{
    return !jobSummary.StatisticsYson && jobSummary.JobExecutionCompleted;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
