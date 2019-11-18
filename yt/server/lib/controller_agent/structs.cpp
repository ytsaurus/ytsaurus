#include "structs.h"

#include <yt/server/lib/controller_agent/serialize.h>

#include <yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent {

using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

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
    , State(static_cast<EJobState>(event->status().state()))
    , FinishTime(event->has_finish_time() ? std::make_optional(FromProto<TInstant>(event->finish_time())) : std::nullopt)
    , LogAndProfile(event->log_and_profile())
{
    auto* status = event->mutable_status();
    Result.Swap(status->mutable_result());
    if (status->has_prepare_duration()) {
        PrepareDuration = FromProto<TDuration>(status->prepare_duration());
    }
    if (status->has_download_duration()) {
        DownloadDuration = FromProto<TDuration>(status->download_duration());
    }
    if (status->has_prepare_root_fs_duration()) {
        PrepareRootFSDuration = FromProto<TDuration>(status->prepare_root_fs_duration());
    }
    if (status->has_exec_duration()) {
        ExecDuration = FromProto<TDuration>(status->exec_duration());
    }
    if (status->has_statistics()) {
        StatisticsYson = TYsonString(status->statistics());
    }
    if (status->has_phase()) {
        Phase = static_cast<EJobPhase>(status->phase());
    }
}

void TJobSummary::Persist(const NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Result);
    Persist(context, Id);
    Persist(context, State);
    Persist(context, FinishTime);
    Persist(context, PrepareDuration);
    Persist(context, DownloadDuration);
    Persist(context, ExecDuration);
    Persist(context, Statistics);
    Persist(context, StatisticsYson);
    Persist(context, LogAndProfile);
    Persist(context, ReleaseFlags);
    Persist(context, PrepareRootFSDuration);
    Persist(context, Phase);
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
            (InterruptReason != EInterruptReason::None && schedulerResultExt.unread_chunk_specs_size() != 0));
}

void TCompletedJobSummary::Persist(const NPhoenix::TPersistenceContext& context)
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
}

////////////////////////////////////////////////////////////////////////////////

TRunningJobSummary::TRunningJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : TJobSummary(event)
    , Progress(event->status().progress())
    , StderrSize(event->status().stderr_size())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
