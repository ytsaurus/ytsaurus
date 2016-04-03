#include "job.h"
#include "exec_node.h"
#include "helpers.h"
#include "operation.h"
#include "operation_controller.h"

#include <yt/core/misc/enum.h>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient::NProto;
using namespace NYTree;
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
    TOperationPtr operation,
    TExecNodePtr node,
    TInstant startTime,
    const TJobResources& resourceLimits,
    bool restarted,
    TJobSpecBuilder specBuilder)
    : Id_(id)
    , Type_(type)
    , Operation_(operation.Get())
    , OperationId_(operation->GetId())
    , Node_(node)
    , StartTime_(startTime)
    , Restarted_(restarted)
    , State_(EJobState::Waiting)
    , ResourceUsage_(resourceLimits)
    , ResourceLimits_(resourceLimits)
    , SpecBuilder_(std::move(specBuilder))
{ }

TDuration TJob::GetDuration() const
{
    return *FinishTime_ - StartTime_;
}

void TJob::SetResult(NJobTrackerClient::NProto::TJobResult&& result)
{
    Result_ = New<TRefCountedJobResult>(std::move(result));
}

const Stroka& TJob::GetStatisticsSuffix() const
{
    auto state = (GetRestarted() && GetState() == EJobState::Completed) ? EJobState::Lost : GetState();
    auto type = GetType();
    return JobHelper.GetStatisticsSuffix(state, type);
}

////////////////////////////////////////////////////////////////////

TJobSummary::TJobSummary(const TJobPtr& job)
    : Result(job->Result())
    , Id(job->GetId())
    , StatisticsSuffix(job->GetStatisticsSuffix())
    , FinishTime(*job->GetFinishTime())
{ }

TJobSummary::TJobSummary(const TJobId& id)
    : Result()
    , Id(id)
    , StatisticsSuffix()
{ }

void TJobSummary::ParseStatistics()
{
    if (Result) {
        Statistics = FromProto<NJobTrackerClient::TStatistics>(Result->statistics());
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
    , AbortReason(GetAbortReason(Result))
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
