#include "stdafx.h"
#include "job.h"
#include "helpers.h"
#include "operation.h"
#include "exec_node.h"
#include "operation_controller.h"

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient::NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& id,
    EJobType type,
    TOperationPtr operation,
    TExecNodePtr node,
    TInstant startTime,
    const TNodeResources& resourceLimits,
    bool restarted,
    TJobSpecBuilder specBuilder)
    : Id_(id)
    , Type_(type)
    , Operation_(operation.Get())
    , Node_(node)
    , StartTime_(startTime)
    , Restarted_(restarted)
    , State_(EJobState::Waiting)
    , ResourceUsage_(resourceLimits)
    , ResourceLimits_(resourceLimits)
    , SpecBuilder_(std::move(specBuilder))
{ }


void TJob::FinalizeJob(const TInstant& finishTime)
{
    FinishTime_ = finishTime;
    Statistics_.Add("/time/total", GetDuration().MilliSeconds());
    if (Result()->has_prepare_time()) {
        Statistics_.Add("/time/prepare", Result()->prepare_time());
    }

    if (Result()->has_exec_time()) {
        Statistics_.Add("/time/exec", Result()->exec_time());
    }
}

TDuration TJob::GetDuration() const
{
    return *FinishTime_ - StartTime_;
}

void TJob::SetResult(NJobTrackerClient::NProto::TJobResult&& result)
{
    Result_ = New<TRefCountedJobResult>(std::move(result));
    Statistics_ = NYTree::ConvertTo<TStatistics>(NYTree::TYsonString(Result_->statistics()));
}

////////////////////////////////////////////////////////////////////

TCompletedJobSummary::TCompletedJobSummary(TJobPtr job)
    : Result(job->Result())
    , Id(job->GetId())
    , Statistics(job->Statistics())
{ }

////////////////////////////////////////////////////////////////////

TFailedJobSummary::TFailedJobSummary(TJobPtr job)
    : Result(job->Result())
    , Id(job->GetId())
{ }

////////////////////////////////////////////////////////////////////

TAbortedJobSummary::TAbortedJobSummary(const TJobId& id, EAbortReason abortReason)
    : Id(id)
    , AbortReason(abortReason)
{ }

TAbortedJobSummary::TAbortedJobSummary(TJobPtr job)
    : Result(job->Result())
    , Id(job->GetId())
    , AbortReason(GetAbortReason(Result))
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

