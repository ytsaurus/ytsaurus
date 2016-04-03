#include "scheduling_context_detail.h"
#include "exec_node.h"
#include "job.h"
#include "config.h"

#include <yt/ytlib/object_client/helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////

TSchedulingContextBase::TSchedulingContextBase(
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs,
    TCellTag cellTag)
    : ResourceUsageDiscount_(ZeroJobResources())
    , ResourceUsage_(node->GetResourceUsage())
    , ResourceLimits_(node->GetResourceLimits())
    , RunningJobs_(runningJobs)
    , Config_(config)
    , CellTag_(cellTag)
    , Node_(node)
    , NodeDescriptor_(Node_->BuildExecDescriptor())
{ }

const TExecNodeDescriptor& TSchedulingContextBase::GetNodeDescriptor() const
{
    return NodeDescriptor_;
}

TJobPtr TSchedulingContextBase::GetStartedJob(const TJobId& jobId) const
{
    // TODO(acid): Is it worth making it more efficient?
    for (const auto& job : StartedJobs_) {
        if (job->GetId() == jobId) {
            return job;
        }
    }
    YUNREACHABLE();
}

bool TSchedulingContextBase::CanStartMoreJobs() const
{
    if (!Node_->HasSpareResources(ResourceUsageDiscount())) {
        return false;
    }

    auto maxJobStarts = Config_->MaxStartedJobsPerHeartbeat;
    if (maxJobStarts && StartedJobs_.size() >= maxJobStarts.Get()) {
        return false;
    }

    return true;
}

bool TSchedulingContextBase::CanSchedule(const TNullable<Stroka>& tag) const
{
    return Node_->CanSchedule(tag);
}

void TSchedulingContextBase::StartJob(TOperationPtr operation, const TJobStartRequest& jobStartRequest)
{
    auto startTime = GetNow();
    auto job = New<TJob>(
        jobStartRequest.Id,
        jobStartRequest.Type,
        operation,
        Node_,
        startTime,
        jobStartRequest.ResourceLimits,
        jobStartRequest.Restarted,
        jobStartRequest.SpecBuilder);
    StartedJobs_.push_back(job);
}

void TSchedulingContextBase::PreemptJob(TJobPtr job)
{
    YCHECK(job->GetNode() == Node_);
    PreemptedJobs_.push_back(job);
}

TJobId TSchedulingContextBase::GenerateJobId()
{
    return MakeRandomId(EObjectType::SchedulerJob, CellTag_);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
