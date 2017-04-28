#include "private.h"
#include "job.h"
#include "exec_node.h"
#include "helpers.h"
#include "operation.h"
#include "operation_controller.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient::NProto;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NYPath;
using namespace NJobTrackerClient;
using namespace NChunkClient::NProto;
using namespace NProto;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////

static class TJobHelper
{
public:
    TJobHelper()
    {
        for (auto state : TEnumTraits<EJobState>::GetDomainValues()) {
            for (auto type : TEnumTraits<EJobType>::GetDomainValues()) {
                StatisticsSuffixes_[state][type] = Format("/$/%lv/%lv", state, type);
            }
        }
    }

    const Stroka& GetStatisticsSuffix(EJobState state, EJobType type) const
    {
        return StatisticsSuffixes_[state][type];
    }

private:
    TEnumIndexedVector<TEnumIndexedVector<Stroka, EJobType>, EJobState> StatisticsSuffixes_;

} JobHelper;

////////////////////////////////////////////////////////////////////

// Returns true if job proxy wasn't stalling and false otherwise.
// This function is related to the suspicious jobs detection.
bool CheckJobActivity(
    const TBriefJobStatisticsPtr& lhs,
    const TBriefJobStatisticsPtr& rhs,
    i64 cpuUsageThreshold,
    double inputPipeIdleTimeFraction)
{
    bool wasActive = lhs->ProcessedInputRowCount < rhs->ProcessedInputRowCount;
    wasActive |= lhs->ProcessedInputUncompressedDataSize < rhs->ProcessedInputUncompressedDataSize;
    wasActive |= lhs->ProcessedInputCompressedDataSize < rhs->ProcessedInputCompressedDataSize;
    wasActive |= lhs->ProcessedOutputRowCount < rhs->ProcessedOutputRowCount;
    wasActive |= lhs->ProcessedOutputUncompressedDataSize < rhs->ProcessedOutputUncompressedDataSize;
    wasActive |= lhs->ProcessedOutputCompressedDataSize < rhs->ProcessedOutputCompressedDataSize;
    if (lhs->JobProxyCpuUsage && rhs->JobProxyCpuUsage) {
        wasActive |= *lhs->JobProxyCpuUsage + cpuUsageThreshold < *rhs->JobProxyCpuUsage;
    }
    if (lhs->InputPipeIdleTime && rhs->InputPipeIdleTime && lhs->Timestamp < rhs->Timestamp) {
        wasActive |= (*rhs->InputPipeIdleTime - *lhs->InputPipeIdleTime) < (rhs->Timestamp - lhs->Timestamp).MilliSeconds() * inputPipeIdleTimeFraction;
    }
    return wasActive;
}

////////////////////////////////////////////////////////////////////

void Serialize(const TBriefJobStatistics& briefJobStatistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Item("timestamp").Value(briefJobStatistics.Timestamp)
        .EndAttributes()
        .BeginMap()
            .Item("processed_input_row_count").Value(briefJobStatistics.ProcessedInputRowCount)
            .Item("processed_input_data_size").Value(briefJobStatistics.ProcessedInputUncompressedDataSize)
            .Item("processed_input_compressed_data_size").Value(briefJobStatistics.ProcessedInputCompressedDataSize)
            .Item("processed_output_data_size").Value(briefJobStatistics.ProcessedOutputUncompressedDataSize)
            .Item("processed_output_compressed_data_size").Value(briefJobStatistics.ProcessedOutputCompressedDataSize)
            .DoIf(static_cast<bool>(briefJobStatistics.InputPipeIdleTime), [&] (TFluentMap fluent) {
                fluent.Item("input_pipe_idle_time").Value(*briefJobStatistics.InputPipeIdleTime);
            })
            .DoIf(static_cast<bool>(briefJobStatistics.JobProxyCpuUsage), [&] (TFluentMap fluent) {
                fluent.Item("job_proxy_cpu_usage").Value(*briefJobStatistics.JobProxyCpuUsage);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& id,
    EJobType type,
    const TOperationId& operationId,
    TExecNodePtr node,
    TInstant startTime,
    const TJobResources& resourceLimits,
    bool restarted,
    bool interruptible,
    TJobSpecBuilder specBuilder,
    const Stroka& account)
    : Id_(id)
    , Type_(type)
    , OperationId_(operationId)
    , Node_(node)
    , StartTime_(startTime)
    , Restarted_(restarted)
    , Interruptible_(interruptible)
    , State_(EJobState::None)
    , ResourceUsage_(resourceLimits)
    , ResourceLimits_(resourceLimits)
    , SpecBuilder_(std::move(specBuilder))
    , LastActivityTime_(startTime)
    , Account_(account)
{ }

TDuration TJob::GetDuration() const
{
    return *FinishTime_ - StartTime_;
}

void TJob::AnalyzeBriefStatistics(
    TDuration suspiciousInactivityTimeout,
    i64 suspiciousCpuUsageThreshold,
    double suspiciousInputPipeIdleTimeFraction,
    const TErrorOr<TBriefJobStatisticsPtr>& briefStatisticsOrError)
{
    if (!briefStatisticsOrError.IsOK()) {
        LOG_WARNING(briefStatisticsOrError, "Not analyzing brief statistics of job due to an error (JobId: %v)");
        return;
    }

    const auto& briefStatistics = briefStatisticsOrError.Value();

    bool wasActive = false;

    if (!BriefStatistics_ || CheckJobActivity(
        BriefStatistics_,
        briefStatistics,
        suspiciousCpuUsageThreshold,
        suspiciousInputPipeIdleTimeFraction))
    {
        wasActive = true;
    }
    BriefStatistics_ = briefStatistics;

    bool wasSuspicious = Suspicious_;
    Suspicious_ = (!wasActive && BriefStatistics_->Timestamp - LastActivityTime_ > suspiciousInactivityTimeout);
    if (!wasSuspicious && Suspicious_) {
        LOG_DEBUG("Found a suspicious job (JobId: %v, LastActivityTime: %v, SuspiciousInactivityTimeout: %v)",
            Id_,
            LastActivityTime_,
            suspiciousInactivityTimeout);
    }

    if (wasActive) {
        LastActivityTime_ = BriefStatistics_->Timestamp;
    }
}

void TJob::SetStatus(TJobStatus* status)
{
    if (status) {
        Status_.Swap(status);
    }
    if (Status_.has_statistics()) {
        StatisticsYson_ = TYsonString(Status_.statistics());
    }
}

const Stroka& TJob::GetStatisticsSuffix() const
{
    auto state = (GetRestarted() && GetState() == EJobState::Completed) ? EJobState::Lost : GetState();
    auto type = GetType();
    return JobHelper.GetStatisticsSuffix(state, type);
}

////////////////////////////////////////////////////////////////////

TJobSummary::TJobSummary(const TJobPtr& job)
    : Result(job->Status().result())
    , Id(job->GetId())
    , StatisticsSuffix(job->GetStatisticsSuffix())
    , FinishTime(job->GetFinishTime())
    , ShouldLog(true)
{
    const auto& status = job->Status();
    if (status.has_prepare_duration()) {
        PrepareDuration = FromProto<TDuration>(status.prepare_duration());
    }
    if (status.has_download_duration()) {
        DownloadDuration = FromProto<TDuration>(status.download_duration());
    }
    if (status.has_exec_duration()) {
        ExecDuration.Emplace();
        FromProto(ExecDuration.GetPtr(), status.exec_duration());
    }
    StatisticsYson = job->StatisticsYson();
}

TJobSummary::TJobSummary(const TJobId& id)
    : Result()
    , Id(id)
    , StatisticsSuffix()
    , ShouldLog(false)
{ }

void TJobSummary::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    using NYT::Save;
    using NYT::Load;

    Persist<TBinaryProtoSerializer>(context, Result);
    Persist(context, Id);
    Persist(context, StatisticsSuffix);
    Persist(context, FinishTime);
    Persist(context, PrepareDuration);
    Persist(context, DownloadDuration);
    Persist(context, ExecDuration);
    Persist(context, Statistics);
    Persist(context, StatisticsYson);
    Persist(context, ShouldLog);
}

void TJobSummary::ParseStatistics()
{
    if (StatisticsYson) {
        Statistics = ConvertTo<NJobTrackerClient::TStatistics>(StatisticsYson);
        // NB: we should remove timestamp from the statistics as it becomes a YSON-attribute
        // when writing it to the event log, but top-level attributes are disallowed in table rows.
        Statistics.SetTimestamp(Null);
    }
}

////////////////////////////////////////////////////////////////////

TCompletedJobSummary::TCompletedJobSummary(const TJobPtr& job, bool abandoned)
    : TJobSummary(job)
    , Abandoned(abandoned)
{
    const auto& schedulerResultExt = Result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
    InterruptReason = job->GetInterruptReason();
    YCHECK((InterruptReason == EInterruptReason::None && schedulerResultExt.unread_input_data_slice_descriptors_size() == 0) ||
        (InterruptReason != EInterruptReason::None && schedulerResultExt.unread_input_data_slice_descriptors_size() != 0));
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
    YCHECK(InterruptReason == EInterruptReason::None);
    Persist(context, SplitJobCount);
}

////////////////////////////////////////////////////////////////////

TAbortedJobSummary::TAbortedJobSummary(const TJobPtr& job)
    : TJobSummary(job)
    , AbortReason(GetAbortReason(job->Status().result()))
{ }

TAbortedJobSummary::TAbortedJobSummary(const TJobId& id, EAbortReason abortReason)
    : TJobSummary(id)
    , AbortReason(abortReason)
{ }

TAbortedJobSummary::TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason)
    : TJobSummary(other)
    , AbortReason(abortReason)
{ }

////////////////////////////////////////////////////////////////////

TJobStatus JobStatusFromError(const TError& error)
{
    auto status = TJobStatus();
    ToProto(status.mutable_result()->mutable_error(), error);
    return status;
}

////////////////////////////////////////////////////////////////////

TJobStartRequest::TJobStartRequest(
    TJobId id,
    EJobType type,
    const TJobResources& resourceLimits,
    bool restarted,
    bool interruptible,
    TJobSpecBuilder specBuilder,
    const Stroka& account)
    : Id(id)
    , Type(type)
    , ResourceLimits(resourceLimits)
    , Restarted(restarted)
    , Interruptible(interruptible)
    , SpecBuilder(std::move(specBuilder))
    , Account(account)
{ }

////////////////////////////////////////////////////////////////////

void TScheduleJobResult::RecordFail(EScheduleJobFailReason reason)
{
    ++Failed[reason];
}

bool TScheduleJobResult::IsBackoffNeeded() const
{
    return
        !JobStartRequest &&
        Failed[EScheduleJobFailReason::NotEnoughResources] == 0 &&
        Failed[EScheduleJobFailReason::NoLocalJobs] == 0;
}

bool TScheduleJobResult::IsScheduleStopNeeded() const
{
    return
        Failed[EScheduleJobFailReason::NotEnoughChunkLists] > 0 ||
        Failed[EScheduleJobFailReason::JobSpecThrottling] > 0;
}

void TScheduleJobStatistics::RecordJobResult(const TScheduleJobResultPtr& scheduleJobResult)
{
    for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
        Failed[reason] += scheduleJobResult->Failed[reason];
    }
    Duration += scheduleJobResult->Duration;
    ++Count;
}

void TScheduleJobStatistics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Failed);
    Persist(context, Duration);
    Persist(context, Count);
}

DECLARE_DYNAMIC_PHOENIX_TYPE(TScheduleJobStatistics, 0x1ba9c7e0);

////////////////////////////////////////////////////////////////////

TJobId MakeJobId(NObjectClient::TCellTag tag, NNodeTrackerClient::TNodeId nodeId)
{
    return MakeId(
        EObjectType::SchedulerJob,
        tag,
        RandomNumber<ui64>(),
        nodeId);
}

NNodeTrackerClient::TNodeId NodeIdFromJobId(const TJobId& jobId)
{
    return jobId.Parts32[0];
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
