#include "scheduling_context_detail.h"
#include "exec_node.h"
#include "job.h"

#include <yt/server/lib/scheduler/config.h>
#include <yt/server/lib/scheduler/structs.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/scheduler/job_resources.h>

namespace NYT::NScheduler {

using namespace NObjectClient;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TSchedulingContextBase::TSchedulingContextBase(
    int nodeShardId,
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs)
    : NodeShardId_(nodeShardId)
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
    std::vector<i64> diskRequests(DiskRequests_);
    diskRequests.push_back(jobResourcesWithQuota.GetDiskQuota());
    return
        CanSatisfyResourceRequest(jobResourcesWithQuota.ToJobResources()) &&
        CanSatisfyDiskRequests(DiskInfo_, diskRequests);
}

bool TSchedulingContextBase::CanStartMoreJobs() const
{
    if (!CanSatisfyResourceRequest(MinSpareNodeResources())) {
        return false;
    }

    auto limit = Config_->MaxStartedJobsPerHeartbeat;
    return !limit || StartedJobs_.size() < *limit;
}

bool TSchedulingContextBase::CanSchedule(const TSchedulingTagFilter& filter) const
{
    return filter.IsEmpty() || filter.CanSchedule(NodeTags_);
}

void TSchedulingContextBase::StartJob(
    const TString& treeId,
    TOperationId operationId,
    TIncarnationId incarnationId,
    const TJobStartDescriptor& startDescriptor,
    EPreemptionMode preemptionMode)
{
    ResourceUsage_ += startDescriptor.ResourceLimits.ToJobResources();
    if (startDescriptor.ResourceLimits.GetDiskQuota() > 0) {
        DiskRequests_.push_back(startDescriptor.ResourceLimits.GetDiskQuota());
    }
    auto startTime = NProfiling::CpuInstantToInstant(GetNow());
    auto job = New<TJob>(
        startDescriptor.Id,
        startDescriptor.Type,
        operationId,
        incarnationId,
        Node_,
        startTime,
        startDescriptor.ResourceLimits.ToJobResources(),
        startDescriptor.Interruptible,
        preemptionMode,
        treeId);
    StartedJobs_.push_back(job);
}

void TSchedulingContextBase::PreemptJob(const TJobPtr& job)
{
    YT_VERIFY(job->GetNode() == Node_);
    PreemptedJobs_.push_back(job);
}

void TSchedulingContextBase::PreemptJobGracefully(const TJobPtr& job)
{
    YT_VERIFY(job->GetNode() == Node_);
    GracefullyPreemptedJobs_.push_back(job);
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

} // namespace NYT::NScheduler
