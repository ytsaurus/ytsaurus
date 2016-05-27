#include "job.h"
#include "exec_node.h"
#include "helpers.h"
#include "operation.h"
#include "operation_controller.h"

#include <yt/core/misc/enum.h>
#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient::NProto;
using namespace NYTree;
using namespace NYson;
using namespace NJobTrackerClient;
using namespace NChunkClient::NProto;

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
    , HasPendingUnregistration_(false)
    , State_(EJobState::Waiting)
    , ResourceUsage_(resourceLimits)
    , ResourceLimits_(resourceLimits)
    , SpecBuilder_(std::move(specBuilder))
{ }

TDuration TJob::GetDuration() const
{
    return *FinishTime_ - StartTime_;
}

void TJob::SetStatus(TRefCountedJobStatusPtr status)
{
    Status_ = std::move(status);
    if (Status_->has_statistics()) {
        StatisticsYson_ = TYsonString(Status_->statistics());
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
    : Result(New<TRefCountedJobResult>(job->Status()->result()))
    , Id(job->GetId())
    , StatisticsSuffix(job->GetStatisticsSuffix())
    , FinishTime(*job->GetFinishTime())
    , ShouldLog(true)
{ 
    const auto& status = job->Status();
    if (status->has_prepare_duration()) {
        PrepareDuration = FromProto<TDuration>(status->prepare_duration());
    }
    if (status->has_exec_duration()) {
        ExecDuration.Emplace();
        FromProto(ExecDuration.GetPtr(), status->exec_duration());
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
    , AbortReason(GetAbortReason(job->Status()->result()))
{ }

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

} // namespace NScheduler
} // namespace NYT
