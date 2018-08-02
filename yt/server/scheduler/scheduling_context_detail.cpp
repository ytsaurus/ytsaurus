#include "scheduling_context_detail.h"
#include "exec_node.h"
#include "job.h"
#include "config.h"

#include <yt/client/object_client/helpers.h>
#include <yt/ytlib/scheduler/job_resources.h>

namespace NYT {
namespace NScheduler {

using namespace NObjectClient;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TSchedulingContextBase::TSchedulingContextBase(
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs)
    : ResourceUsageDiscount_(ZeroJobResources())
    , ResourceUsage_(node->GetResourceUsage())
    , ResourceLimits_(node->GetResourceLimits())
    , DiskInfo_(node->GetDiskInfo())
    , RunningJobs_(runningJobs)
    , Config_(std::move(config))
    , Node_(std::move(node))
    , NodeDescriptor_(Node_->BuildExecDescriptor())
    , NodeTags_(Node_->Tags())
{ }

const TExecNodeDescriptor& TSchedulingContextBase::GetNodeDescriptor() const
{
    return NodeDescriptor_;
}

bool TSchedulingContextBase::CanSatisfyResourceRequest(const TJobResources& jobResources) const
{
    return Dominates(
        ResourceLimits_,
        ResourceUsage_ + jobResources - ResourceUsageDiscount_);
}

bool TSchedulingContextBase::CanStartJob(const TJobResourcesWithQuota& jobResourcesWithQuota) const
{
    return
        CanSatisfyResourceRequest(jobResourcesWithQuota.ToJobResources()) &&
        CanSatisfyDiskRequest(DiskInfo_, jobResourcesWithQuota.GetDiskQuota());
}

bool TSchedulingContextBase::CanStartMoreJobs() const
{
    if (!CanSatisfyResourceRequest(MinSpareNodeResources())) {
        return false;
    }

    auto maxJobStarts = Config_->MaxStartedJobsPerHeartbeat;
    return !maxJobStarts.HasValue() || StartedJobs_.size() < maxJobStarts.Get();
}

bool TSchedulingContextBase::CanSchedule(const TSchedulingTagFilter& filter) const
{
    return filter.IsEmpty() || filter.CanSchedule(NodeTags_);
}

void TSchedulingContextBase::StartJob(
    const TString& treeId,
    const TOperationId& operationId,
    const TIncarnationId& incarnationId,
    const TJobStartDescriptor& startDescriptor)
{
    auto startTime = NProfiling::CpuInstantToInstant(GetNow());
    auto job = New<TJob>(
        startDescriptor.Id,
        startDescriptor.Type,
        operationId,
        incarnationId,
        Node_,
        startTime,
        startDescriptor.ResourceLimits,
        startDescriptor.Interruptible,
        treeId);
    StartedJobs_.push_back(job);
}

void TSchedulingContextBase::PreemptJob(const TJobPtr& job)
{
    YCHECK(job->GetNode() == Node_);
    PreemptedJobs_.push_back(job);
}

TJobResources TSchedulingContextBase::GetNodeFreeResourcesWithoutDiscount()
{
    return ResourceLimits_ - ResourceUsage_;
}

TJobResources TSchedulingContextBase::GetNodeFreeResourcesWithDiscount()
{
    return ResourceLimits_ - ResourceUsage_ + ResourceUsageDiscount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
