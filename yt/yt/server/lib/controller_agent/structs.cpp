#include "public.h"
#include "structs.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/server/lib/exec_node/public.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/ytlib/job_proxy/public.h>

#include <yt/yt/ytlib/job_tracker_client/statistics.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/misc/variant.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent {

using namespace NScheduler;
using namespace NYson;
using namespace NYTree;
using namespace NLogging;
using namespace NJobTrackerClient;

using NYT::FromProto;
using NYT::ToProto;
using NScheduler::NProto::TSchedulerJobResultExt;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void MergeJobSummaries(
    TJobSummary& nodeJobSummary,
    TFinishedJobSummary&& schedulerJobSummary)
{
    YT_VERIFY(nodeJobSummary.Id == schedulerJobSummary.Id);

    nodeJobSummary.FinishTime = schedulerJobSummary.FinishTime;
}

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

void JobEventsCommonPartToProto(auto* proto, const auto& summary)
{
    ToProto(proto->mutable_operation_id(), summary.OperationId);
    ToProto(proto->mutable_job_id(), summary.Id);
}

void JobEventsCommonPartFromProto(auto* summary, auto* protoEvent)
{
    summary->OperationId = FromProto<TOperationId>(protoEvent->operation_id());
    summary->Id = FromProto<TJobId>(protoEvent->job_id());
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
    TimeStatistics = FromProto<NJobAgent::TTimeStatistics>(status->time_statistics());
    if (status->has_statistics()) {
        auto mutableStatistics = std::make_shared<TStatistics>();
        *mutableStatistics = ConvertTo<TStatistics>(TYsonStringBuf(status->statistics()));
        Statistics = std::move(mutableStatistics);
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
}

void TJobSummary::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Result);
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

TSchedulerJobResultExt& TJobSummary::GetSchedulerJobResult()
{
    YT_VERIFY(Result);
    YT_VERIFY(Result->HasExtension(TSchedulerJobResultExt::job_result_ext));
    return *Result->MutableExtension(TSchedulerJobResultExt::job_result_ext);
}

const TSchedulerJobResultExt& TJobSummary::GetSchedulerJobResult() const
{
    YT_VERIFY(Result);
    YT_VERIFY(Result->HasExtension(TSchedulerJobResultExt::job_result_ext));
    return Result->GetExtension(TSchedulerJobResultExt::job_result_ext);
}

const TSchedulerJobResultExt* TJobSummary::FindSchedulerJobResult() const
{
    YT_VERIFY(Result);
    return Result->HasExtension(TSchedulerJobResultExt::job_result_ext)
        ? &Result->GetExtension(TSchedulerJobResultExt::job_result_ext)
        : nullptr;
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

    if (auto error = NYT::FromProto<TError>(GetJobResult().error()); !error.IsOK()) {
        AbortReason = GetAbortReason(error, Logger);
    }

    if (status->has_preempted_for()) {
        PreemptedFor = FromProto<NScheduler::TPreemptedFor>(status->preempted_for());
    }
}

std::unique_ptr<TAbortedJobSummary> CreateAbortedJobSummary(TAbortedBySchedulerJobSummary&& eventSummary, const TLogger& Logger)
{
    auto abortReason = [&] {
        if (eventSummary.AbortReason) {
            return *eventSummary.AbortReason;
        }

        return GetAbortReason(eventSummary.Error, Logger);
    }();

    TAbortedJobSummary summary{eventSummary.Id, abortReason};

    summary.FinishTime = eventSummary.FinishTime;

    ToProto(summary.Result.emplace().mutable_error(), eventSummary.Error);

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

TRunningJobSummary::TRunningJobSummary(NProto::TJobStatus* status)
    : TJobSummary(status)
    , Progress(status->progress())
    , StderrSize(status->stderr_size())
{ }

////////////////////////////////////////////////////////////////////////////////

void ToProto(NScheduler::NProto::TSchedulerToAgentFinishedJobEvent* protoEvent, const TFinishedJobSummary& finishedJobSummary)
{
    JobEventsCommonPartToProto(protoEvent, finishedJobSummary);
    protoEvent->set_finish_time(ToProto<ui64>(finishedJobSummary.FinishTime));
}

void FromProto(TFinishedJobSummary* finishedJobSummary, NScheduler::NProto::TSchedulerToAgentFinishedJobEvent* protoEvent)
{
    JobEventsCommonPartFromProto(finishedJobSummary, protoEvent);
    finishedJobSummary->FinishTime = FromProto<TInstant>(protoEvent->finish_time());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NScheduler::NProto::TSchedulerToAgentAbortedJobEvent* protoEvent, const TAbortedBySchedulerJobSummary& abortedJobSummary)
{
    JobEventsCommonPartToProto(protoEvent, abortedJobSummary);
    protoEvent->set_finish_time(ToProto<ui64>(abortedJobSummary.FinishTime));
    if (abortedJobSummary.AbortReason) {
        protoEvent->set_abort_reason(static_cast<int>(*abortedJobSummary.AbortReason));
    }
    ToProto(protoEvent->mutable_error(), abortedJobSummary.Error);
    protoEvent->set_scheduled(abortedJobSummary.Scheduled);
}

void FromProto(TAbortedBySchedulerJobSummary* abortedJobSummary, NScheduler::NProto::TSchedulerToAgentAbortedJobEvent* protoEvent)
{
    JobEventsCommonPartFromProto(abortedJobSummary, protoEvent);
    abortedJobSummary->FinishTime = FromProto<TInstant>(protoEvent->finish_time());
    if (protoEvent->has_abort_reason()) {
        abortedJobSummary->AbortReason = CheckedEnumCast<EAbortReason>(protoEvent->abort_reason());
    }
    abortedJobSummary->Error = FromProto<TError>(protoEvent->error());
    abortedJobSummary->Scheduled = protoEvent->scheduled();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NScheduler::NProto::TSchedulerToAgentJobEvent* proto, const TSchedulerToAgentJobEvent& event)
{
    Visit(
        event.EventSummary,
        [&] (const TFinishedJobSummary& summary) {
            ToProto(proto->mutable_finished(), summary);
        },
        [&] (const TAbortedBySchedulerJobSummary& summary) {
            ToProto(proto->mutable_aborted_by_scheduler(), summary);
        });
}

void FromProto(TSchedulerToAgentJobEvent* event, NScheduler::NProto::TSchedulerToAgentJobEvent* proto)
{
    using TProtoMessage = NScheduler::NProto::TSchedulerToAgentJobEvent;
    switch (proto->job_event_case()) {
        case TProtoMessage::JobEventCase::kFinished: {
            TFinishedJobSummary summary;
            FromProto(&summary, proto->mutable_finished());
            event->EventSummary = std::move(summary);
            return;
        }
        case TProtoMessage::JobEventCase::kAbortedByScheduler: {
            TAbortedBySchedulerJobSummary summary;
            FromProto(&summary, proto->mutable_aborted_by_scheduler());
            event->EventSummary = std::move(summary);
            return;
        }
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TFailedJobSummary> MergeJobSummaries(
    std::unique_ptr<TFailedJobSummary> nodeJobSummary,
    TFinishedJobSummary&& schedulerJobSummary,
    const TLogger& /*Logger*/)
{
    MergeJobSummaries(*nodeJobSummary, std::move(schedulerJobSummary));

    return nodeJobSummary;
}

std::unique_ptr<TAbortedJobSummary> MergeJobSummaries(
    std::unique_ptr<TAbortedJobSummary> nodeJobSummary,
    TFinishedJobSummary&& schedulerJobSummary,
    const TLogger& /*Logger*/)
{
    MergeJobSummaries(*nodeJobSummary, std::move(schedulerJobSummary));

    return nodeJobSummary;
}

std::unique_ptr<TCompletedJobSummary> MergeJobSummaries(
    std::unique_ptr<TCompletedJobSummary> nodeJobSummary,
    TFinishedJobSummary&& schedulerJobSummary,
    const TLogger& /*Logger*/)
{
    MergeJobSummaries(*nodeJobSummary, std::move(schedulerJobSummary));

    return nodeJobSummary;
}

std::unique_ptr<TJobSummary> MergeJobSummaries(
    std::unique_ptr<TJobSummary> nodeJobSummary,
    TFinishedJobSummary&& schedulerJobSummary,
    const TLogger& Logger)
{
    switch (nodeJobSummary->State) {
        case EJobState::Aborted:
            return MergeJobSummaries(
                SummaryCast<TAbortedJobSummary>(std::move(nodeJobSummary)),
                std::move(schedulerJobSummary),
                Logger);
        case EJobState::Completed:
            return MergeJobSummaries(
                SummaryCast<TCompletedJobSummary>(std::move(nodeJobSummary)),
                std::move(schedulerJobSummary),
                Logger);
        case EJobState::Failed:
            return MergeJobSummaries(
                SummaryCast<TFailedJobSummary>(std::move(nodeJobSummary)),
                std::move(schedulerJobSummary),
                Logger);
        default:
            YT_ABORT();
    }
}

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
