#include "public.h"
#include "structs.h"
#include "statistics.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/server/lib/exec_node/public.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/ytlib/job_proxy/public.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/job_tracker_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent {

using namespace NScheduler;
using namespace NYson;
using namespace NYTree;
using namespace NLogging;

using NYT::FromProto;
using NYT::ToProto;
using NControllerAgent::NProto::TJobResultExt;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

EAbortReason GetAbortReason(const TError& resultError, const TLogger& Logger)
{
    try {
        return resultError.Attributes().Get<EAbortReason>("abort_reason", EAbortReason::Scheduler);
    } catch (const std::exception& ex) {
        // Process unknown abort reason from node.
        YT_LOG_WARNING(ex, "Found unknown abort reason in job result");
        return EAbortReason::Unknown;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TJobSummary::TJobSummary(TJobId id, EJobState state)
    : Result()
    , Id(id)
    , State(state)
{ }

TJobSummary::TJobSummary(NProto::TJobStatus* status)
    : Id(FromProto<TJobId>(status->job_id()))
    , State(CheckedEnumCast<EJobState>(status->state()))
    , FinishedOnNode(true)
{
    Result = std::move(*status->mutable_result());
    Error = FromProto<TError>(Result->error());

    TimeStatistics = FromProto<NJobAgent::TTimeStatistics>(status->time_statistics());
    if (status->has_statistics()) {
        auto mutableStatistics = std::make_shared<TStatistics>();
        *mutableStatistics = ConvertTo<TStatistics>(TYsonStringBuf(status->statistics()));
        Statistics = std::move(mutableStatistics);
    }

    if (status->has_start_time()) {
        StartTime = FromProto<TInstant>(status->start_time());
    }

    if (status->has_total_input_data_statistics()) {
        TotalInputDataStatistics = FromProto<NChunkClient::NProto::TDataStatistics>(status->total_input_data_statistics());
        OutputDataStatistics = FromProto<std::vector<NChunkClient::NProto::TDataStatistics>>(status->output_data_statistics());
        TotalOutputDataStatistics.emplace();
        for (const auto& statistics : *OutputDataStatistics) {
            *TotalOutputDataStatistics += statistics;
        }
    } else {
        // COMPAT(max42): remove this when all nodes are 22.4+.
        FillDataStatisticsFromStatistics();
    }

    if (status->has_phase()) {
        Phase = CheckedEnumCast<EJobPhase>(status->phase());
    }

    StatusTimestamp = FromProto<TInstant>(status->status_timestamp());
    JobExecutionCompleted = status->job_execution_completed();

    if (NJobTrackerClient::IsJobFinished(State)) {
        FinishTime = TInstant::Now();
    }
}

void TJobSummary::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Result);
    if (context.GetVersion() >= ESnapshotVersion::JobErrorInJobSummary) {
        Persist(context, Error);
    } else if (context.IsLoad() && Result) {
        Error = FromProto<TError>(Result->error());
    }

    Persist(context, Id);
    Persist(context, State);
    Persist(context, FinishTime);
    if (context.GetVersion() < ESnapshotVersion::DoNotPersistStatistics) {
        std::optional<TStatistics> dummyStatistics;
        Persist(context, dummyStatistics);

        TYsonString dummyYson;
        Persist(context, dummyYson);
    }
    Persist(context, ReleaseFlags);
    Persist(context, Phase);
    Persist(context, TimeStatistics);

    if (context.GetVersion() >= ESnapshotVersion::PersistDataStatistics) {
        Persist(context, TotalInputDataStatistics);
        Persist(context, OutputDataStatistics);
        Persist(context, TotalOutputDataStatistics);
    }
}

NProto::TJobResult& TJobSummary::GetJobResult()
{
    YT_VERIFY(Result);
    return *Result;
}

const NProto::TJobResult& TJobSummary::GetJobResult() const
{
    YT_VERIFY(Result);
    return *Result;
}

TJobResultExt& TJobSummary::GetJobResultExt()
{
    YT_VERIFY(Result);
    YT_VERIFY(Result->HasExtension(TJobResultExt::job_result_ext));
    return *Result->MutableExtension(TJobResultExt::job_result_ext);
}

const TJobResultExt& TJobSummary::GetJobResultExt() const
{
    YT_VERIFY(Result);
    YT_VERIFY(Result->HasExtension(TJobResultExt::job_result_ext));
    return Result->GetExtension(TJobResultExt::job_result_ext);
}

const TJobResultExt* TJobSummary::FindJobResultExt() const
{
    YT_VERIFY(Result);
    return Result->HasExtension(TJobResultExt::job_result_ext)
        ? &Result->GetExtension(TJobResultExt::job_result_ext)
        : nullptr;
}

const TError& TJobSummary::GetError() const
{
    YT_VERIFY(Error);
    return *Error;
}

void TJobSummary::FillDataStatisticsFromStatistics()
{
    if (!Statistics) {
        return;
    }

    TotalInputDataStatistics = GetTotalInputDataStatistics(*Statistics);
    OutputDataStatistics = GetOutputDataStatistics(*Statistics);

    TotalOutputDataStatistics.emplace();
    for (const auto& statistics : *OutputDataStatistics) {
        *TotalOutputDataStatistics += statistics;
    }
}

////////////////////////////////////////////////////////////////////////////////

TCompletedJobSummary::TCompletedJobSummary(NControllerAgent::NProto::TJobStatus* status)
    : TJobSummary(status)
    , InterruptReason(status->has_interruption_reason()
        ? CheckedEnumCast<EInterruptReason>(status->interruption_reason())
        : EInterruptReason::None)
{
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

std::unique_ptr<TCompletedJobSummary> CreateAbandonedJobSummary(TJobId jobId)
{
    TCompletedJobSummary summary{};

    summary.Statistics = std::make_shared<TStatistics>();
    summary.Id = jobId;
    summary.State = EJobState::Completed;
    summary.Abandoned = true;
    summary.FinishTime = TInstant::Now();

    return std::make_unique<TCompletedJobSummary>(std::move(summary));
}

////////////////////////////////////////////////////////////////////////////////

TAbortedJobSummary::TAbortedJobSummary(TJobId id, EAbortReason abortReason)
    : TJobSummary(id, EJobState::Aborted)
    , AbortReason(abortReason)
    , AbortInitiator(EJobAbortInitiator::ControllerAgent)
{
    FinishTime = TInstant::Now();
}

TAbortedJobSummary::TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason)
    : TJobSummary(other)
    , AbortReason(abortReason)
    , AbortInitiator(EJobAbortInitiator::ControllerAgent)
{
    State = EJobState::Aborted;
    FinishTime = TInstant::Now();
}

TAbortedJobSummary::TAbortedJobSummary(NProto::TJobStatus* status, const NLogging::TLogger& Logger)
    : TJobSummary(status)
{
    YT_VERIFY(State == ExpectedState);

    if (const auto& error = GetError(); !error.IsOK()) {
        AbortReason = GetAbortReason(error, Logger);
    }

    if (status->has_preempted_for()) {
        PreemptedFor = FromProto<NScheduler::TPreemptedFor>(status->preempted_for());
    }
}

std::unique_ptr<TAbortedJobSummary> CreateAbortedJobSummary(
    TJobId jobId,
    TAbortedAllocationSummary&& eventSummary)
{
    TAbortedJobSummary summary{jobId, eventSummary.AbortReason};

    summary.FinishTime = eventSummary.FinishTime;

    ToProto(summary.Result.emplace().mutable_error(), eventSummary.Error);
    summary.Error = std::move(eventSummary.Error);

    summary.Scheduled = eventSummary.Scheduled;
    summary.AbortInitiator = EJobAbortInitiator::Scheduler;

    return std::make_unique<TAbortedJobSummary>(std::move(summary));
}

////////////////////////////////////////////////////////////////////////////////

TFailedJobSummary::TFailedJobSummary(NProto::TJobStatus* status)
    : TJobSummary(status)
{
    YT_VERIFY(State == ExpectedState);
}

////////////////////////////////////////////////////////////////////////////////

TWaitingJobSummary::TWaitingJobSummary(NProto::TJobStatus* status)
    : TJobSummary(status)
{
    YT_VERIFY(State == ExpectedState);
}

////////////////////////////////////////////////////////////////////////////////

TRunningJobSummary::TRunningJobSummary(NProto::TJobStatus* status)
    : TJobSummary(status)
    , Progress(status->progress())
    , StderrSize(status->stderr_size())
{ }

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NScheduler::NProto::TSchedulerToAgentAbortedAllocationEvent* protoEvent,
    const TAbortedAllocationSummary& abortedAllocationSummary)
{
    ToProto(protoEvent->mutable_operation_id(), abortedAllocationSummary.OperationId);
    ToProto(protoEvent->mutable_allocation_id(), abortedAllocationSummary.Id);

    protoEvent->set_finish_time(ToProto<ui64>(abortedAllocationSummary.FinishTime));
    protoEvent->set_abort_reason(static_cast<int>(abortedAllocationSummary.AbortReason));
    ToProto(protoEvent->mutable_error(), abortedAllocationSummary.Error);
    protoEvent->set_scheduled(abortedAllocationSummary.Scheduled);
}

void FromProto(
    TAbortedAllocationSummary* abortedAllocationSummary,
    NScheduler::NProto::TSchedulerToAgentAbortedAllocationEvent* protoEvent)
{
    abortedAllocationSummary->OperationId = FromProto<TOperationId>(protoEvent->operation_id());
    abortedAllocationSummary->Id = FromProto<TAllocationId>(protoEvent->allocation_id());

    abortedAllocationSummary->FinishTime = FromProto<TInstant>(protoEvent->finish_time());
    abortedAllocationSummary->AbortReason = CheckedEnumCast<EAbortReason>(protoEvent->abort_reason());

    abortedAllocationSummary->Error = FromProto<TError>(protoEvent->error());
    abortedAllocationSummary->Scheduled = protoEvent->scheduled();
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TJobSummary> ParseJobSummary(NProto::TJobStatus* const status, const TLogger& Logger)
{
    const auto state = static_cast<EJobState>(status->state());
    switch (state) {
        case EJobState::Completed:
            return std::make_unique<TCompletedJobSummary>(status);
        case EJobState::Failed:
            return std::make_unique<TFailedJobSummary>(status);
        case EJobState::Aborted:
            return std::make_unique<TAbortedJobSummary>(status, Logger);
        case EJobState::Running:
            return std::make_unique<TRunningJobSummary>(status);
        case EJobState::Waiting:
            return std::make_unique<TWaitingJobSummary>(status);
        default:
            YT_LOG_ERROR(
                "Unexpected job state in parsing status (JobState: %v, JobId: %v)",
                state,
                FromProto<TJobId>(status->job_id()));
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
