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

bool CompareBriefJobStatistics(
    const TBriefJobStatisticsPtr& lhs,
    const TBriefJobStatisticsPtr& rhs,
    i64 userJobCpuUsageThreshold,
    i64 userJobIOReadThreshold)
{
    return lhs->ProcessedInputRowCount < rhs->ProcessedInputRowCount ||
        lhs->ProcessedInputDataSize < rhs->ProcessedInputDataSize ||
        lhs->ProcessedOutputRowCount < rhs->ProcessedOutputRowCount ||
        lhs->ProcessedOutputDataSize < rhs->ProcessedOutputDataSize ||
        (lhs->UserJobCpuUsage && rhs->UserJobCpuUsage &&
        *lhs->UserJobCpuUsage + userJobCpuUsageThreshold < *rhs->UserJobCpuUsage) ||
        (lhs->UserJobBlockIORead && rhs->UserJobBlockIORead &&
        *lhs->UserJobBlockIORead + userJobIOReadThreshold < *rhs->UserJobBlockIORead);
}

////////////////////////////////////////////////////////////////////

void Serialize(const TBriefJobStatistics& briefJobStatistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("processed_input_row_count").Value(briefJobStatistics.ProcessedInputRowCount)
            .Item("processed_input_data_size").Value(briefJobStatistics.ProcessedInputDataSize)
            .Item("processed_output_data_size").Value(briefJobStatistics.ProcessedOutputDataSize)
            .DoIf(static_cast<bool>(briefJobStatistics.UserJobBlockIORead), [&] (TFluentMap fluent) {
                fluent.Item("user_job_io_read").Value(*briefJobStatistics.UserJobBlockIORead);
            })
            .DoIf(static_cast<bool>(briefJobStatistics.UserJobCpuUsage), [&] (TFluentMap fluent) {
                fluent.Item("user_job_cpu_usage").Value(*briefJobStatistics.UserJobCpuUsage);
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
    TJobSpecBuilder specBuilder)
    : Id_(id)
    , Type_(type)
    , OperationId_(operationId)
    , Node_(node)
    , StartTime_(startTime)
    , Restarted_(restarted)
    , State_(EJobState::None)
    , ResourceUsage_(resourceLimits)
    , ResourceLimits_(resourceLimits)
    , SpecBuilder_(std::move(specBuilder))
    , LastActivityTime_(startTime)
{ }

TDuration TJob::GetDuration() const
{
    return *FinishTime_ - StartTime_;
}

TBriefJobStatisticsPtr TJob::BuildBriefStatistics(const TYsonString& statisticsYson) const
{
    auto statistics = ConvertTo<NJobTrackerClient::TStatistics>(statisticsYson);

    auto briefStatistics = New<TBriefJobStatistics>();
    briefStatistics->ProcessedInputRowCount = GetNumericValue(statistics, "/data/input/row_count");
    briefStatistics->ProcessedInputDataSize = GetNumericValue(statistics, "/data/input/uncompressed_data_size");
    briefStatistics->UserJobCpuUsage = FindNumericValue(statistics, "/user_job/cpu/user");
    briefStatistics->UserJobBlockIORead = FindNumericValue(statistics, "/user_job/blkio/io_read");

    auto outputDataStatistics = GetTotalOutputDataStatistics(statistics);
    briefStatistics->ProcessedOutputDataSize = outputDataStatistics.uncompressed_data_size();
    briefStatistics->ProcessedOutputRowCount = outputDataStatistics.row_count();

    return briefStatistics;
}

void TJob::AnalyzeBriefStatistics(
    TDuration suspiciousInactivityTimeout,
    i64 suspiciousUserJobCpuUsageThreshold,
    i64 suspiciousUserJobBlockIOReadThreshold,
    const TBriefJobStatisticsPtr& briefStatistics)
{
    bool wasActive = false;

    if (!BriefStatistics_ || CompareBriefJobStatistics(
        BriefStatistics_,
        briefStatistics,
        suspiciousUserJobCpuUsageThreshold,
        suspiciousUserJobBlockIOReadThreshold))
    {
        wasActive = true;
    }
    BriefStatistics_ = briefStatistics;

    bool wasSuspicious = Suspicious_;
    Suspicious_ = (!wasActive && TInstant::Now() - LastActivityTime_ > suspiciousInactivityTimeout);
    if (!wasSuspicious && Suspicious_) {
        LOG_DEBUG("Found a suspicious job (JobId: %v, LastActivityTime: %v, SuspiciousInactivityTimeout: %v)",
            Id_,
            LastActivityTime_,
            suspiciousInactivityTimeout);
    }

    if (wasActive) {
        LastActivityTime_ = TInstant::Now();
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
    , FinishTime(*job->GetFinishTime())
    , ShouldLog(true)
{
    const auto& status = job->Status();
    if (status.has_prepare_duration()) {
        PrepareDuration = FromProto<TDuration>(status.prepare_duration());
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

void TJobSummary::ParseStatistics()
{
    if (StatisticsYson) {
        Statistics = ConvertTo<NJobTrackerClient::TStatistics>(*StatisticsYson);
    }
}

////////////////////////////////////////////////////////////////////

TCompletedJobSummary::TCompletedJobSummary(const TJobPtr& job, bool abandoned)
    : TJobSummary(job)
    , Abandoned(abandoned)
{ }

////////////////////////////////////////////////////////////////////

TAbortedJobSummary::TAbortedJobSummary(const TJobId& id, EAbortReason abortReason)
    : TJobSummary(id)
    , AbortReason(abortReason)
{ }

TAbortedJobSummary::TAbortedJobSummary(const TJobPtr& job)
    : TJobSummary(job)
    , AbortReason(GetAbortReason(job->Status().result()))
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
    const TJobSpecBuilder& specBuilder)
    : Id(id)
    , Type(type)
    , ResourceLimits(resourceLimits)
    , Restarted(restarted)
    , SpecBuilder(specBuilder)
{ }

////////////////////////////////////////////////////////////////////

void TScheduleJobResult::RecordFail(EScheduleJobFailReason reason)
{
    ++Failed[reason];
}

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
