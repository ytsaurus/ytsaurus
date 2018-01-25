#include "scheduling_context_detail.h"
#include "exec_node.h"
#include "job.h"
#include "config.h"

#include <yt/ytlib/object_client/helpers.h>
#include <yt/ytlib/scheduler/job_resources.h>

namespace NYT {
namespace NScheduler {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TSchedulingContextBase::TSchedulingContextBase(
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs,
    TCellTag cellTag)
    : ResourceUsageDiscount_(ZeroJobResources())
    , ResourceUsage_(node->GetResourceUsage())
    , ResourceLimits_(node->GetResourceLimits())
    , DiskInfo_(node->GetDiskInfo())
    , RunningJobs_(runningJobs)
    , Config_(config)
    , CellTag_(cellTag)
    , Node_(node)
    , NodeDescriptor_(Node_->BuildExecDescriptor())
    , NodeTags_(Node_->Tags())
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
    Y_UNREACHABLE();
}

bool TSchedulingContextBase::HasEnoughResources(const TJobResources& neededResources) const
{
    return Dominates(
        ResourceLimits_,
        ResourceUsage_ + neededResources);
}

bool TSchedulingContextBase::CanStartJob(const TJobResources& jobResources) const
{
    return HasEnoughResources(jobResources - ResourceUsageDiscount());
}

bool TSchedulingContextBase::CanStartJobWithQuota(const TJobResourcesWithQuota& jobResourcesWithQuota) const
{
    if (!CanStartJob(jobResourcesWithQuota.ToJobResources())) {
        return false;
    }

    return CanSatisfyDiskRequest(DiskInfo_, jobResourcesWithQuota.GetDiskQuota());
}

bool TSchedulingContextBase::CanStartMoreJobs() const
{
    if (!CanStartJob(MinSpareNodeResources())) {
        return false;
    }

    auto maxJobStarts = Config_->MaxStartedJobsPerHeartbeat;
    if (maxJobStarts && StartedJobs_.size() >= maxJobStarts.Get()) {
        return false;
    }

    return true;
}

bool TSchedulingContextBase::CanSchedule(const TSchedulingTagFilter& filter) const
{
    return filter.IsEmpty() || filter.CanSchedule(NodeTags_);
}

TJobPtr TSchedulingContextBase::StartJob(
    const TString& treeId,
    const TOperationId& operationId,
    const TJobStartRequest& jobStartRequest)
{
    auto startTime = NProfiling::CpuInstantToInstant(GetNow());
    auto job = New<TJob>(
        jobStartRequest.Id,
        jobStartRequest.Type,
        operationId,
        Node_,
        startTime,
        jobStartRequest.ResourceLimits,
        jobStartRequest.Interruptible,
        treeId);
    StartedJobs_.push_back(job);
    return job;
}

void TSchedulingContextBase::PreemptJob(TJobPtr job)
{
    YCHECK(job->GetNode() == Node_);
    PreemptedJobs_.push_back(job);
}

TJobId TSchedulingContextBase::GenerateJobId()
{
    return MakeJobId(CellTag_, Node_->GetId());
}

TJobResources TSchedulingContextBase::GetFreeResources()
{
    return ResourceLimits_ - ResourceUsage_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
