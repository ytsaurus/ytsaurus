#include "scheduling_context_detail.h"

#include "exec_node.h"
#include "allocation.h"
#include "private.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/client/node_tracker_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NScheduler {

using namespace NObjectClient;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TSchedulingContextBase::TSchedulingContextBase(
    int nodeShardId,
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
    : NodeShardId_(nodeShardId)
    , Config_(std::move(config))
    , Node_(std::move(node))
    , NodeDescriptor_(Node_->BuildExecDescriptor())
    , NodeTags_(Node_->Tags())
    , MediumDirectory_(mediumDirectory)
    , DefaultMinSpareJobResources_(
        Config_->MinSpareJobResourcesOnNode
        ? ToJobResources(*Config_->MinSpareJobResourcesOnNode, TJobResources())
        : TJobResources())
    , ResourceUsage_(Node_->GetResourceUsage())
    , ResourceLimits_(Node_->GetResourceLimits())
    , DiskResources_(Node_->GetDiskResources())
    , RunningJobs_(runningJobs)
{ }

int TSchedulingContextBase::GetNodeShardId() const
{
    return NodeShardId_;
}

TJobResources& TSchedulingContextBase::UnconditionalResourceUsageDiscount()
{
    return UnconditionalResourceUsageDiscount_;
}

TJobResources TSchedulingContextBase::GetMaxConditionalUsageDiscount() const
{
    return MaxConditionalUsageDiscount_;
}

TJobResources& TSchedulingContextBase::ResourceUsage()
{
    return ResourceUsage_;
}

const TJobResources& TSchedulingContextBase::ResourceUsage() const
{
    return ResourceUsage_;
}

const TJobResources& TSchedulingContextBase::ResourceLimits() const
{
    return ResourceLimits_;
}

const NNodeTrackerClient::NProto::TDiskResources& TSchedulingContextBase::DiskResources() const
{
    return DiskResources_;
}

NNodeTrackerClient::NProto::TDiskResources& TSchedulingContextBase::DiskResources()
{
    return DiskResources_;
}

const TExecNodeDescriptorPtr& TSchedulingContextBase::GetNodeDescriptor() const
{
    return NodeDescriptor_;
}

bool TSchedulingContextBase::CanSatisfyResourceRequest(
    const TJobResources& jobResources,
    const TJobResources& conditionalDiscount) const
{
    return Dominates(
        ResourceLimits_,
        ResourceUsage_ + jobResources - (UnconditionalResourceUsageDiscount_ + conditionalDiscount));
}

bool TSchedulingContextBase::CanStartJobForOperation(
    const TJobResourcesWithQuota& jobResourcesWithQuota,
    TOperationId operationId) const
{
    std::vector<NScheduler::TDiskQuota> diskRequests(DiskRequests_);
    diskRequests.push_back(jobResourcesWithQuota.DiskQuota());
    return
        CanSatisfyResourceRequest(
            jobResourcesWithQuota.ToJobResources(),
            GetConditionalDiscountForOperation(operationId)) &&
        CanSatisfyDiskQuotaRequests(DiskResources_, diskRequests);
}

bool TSchedulingContextBase::CanStartMoreJobs(const std::optional<TJobResources>& customMinSpareJobResources) const
{
    auto minSpareJobResources = customMinSpareJobResources.value_or(DefaultMinSpareJobResources_);
    if (!CanSatisfyResourceRequest(minSpareJobResources, MaxConditionalUsageDiscount_)) {
        return false;
    }

    auto limit = Config_->MaxStartedJobsPerHeartbeat;
    return !limit || std::ssize(StartedJobs_) < *limit;
}

bool TSchedulingContextBase::CanSchedule(const TSchedulingTagFilter& filter) const
{
    return filter.IsEmpty() || filter.CanSchedule(NodeTags_);
}

bool TSchedulingContextBase::ShouldAbortJobsSinceResourcesOvercommit() const
{
    bool resourcesOvercommitted = !Dominates(ResourceLimits(), ResourceUsage());
    auto now = NProfiling::CpuInstantToInstant(GetNow());
    bool allowedOvercommitTimePassed = Node_->GetResourcesOvercommitStartTime()
        ? Node_->GetResourcesOvercommitStartTime() + Config_->AllowedNodeResourcesOvercommitDuration < now
        : false;
    return resourcesOvercommitted && allowedOvercommitTimePassed;
}

const std::vector<TJobPtr>& TSchedulingContextBase::StartedJobs() const
{
    return StartedJobs_;
}

const std::vector<TJobPtr>& TSchedulingContextBase::RunningJobs() const
{
    return RunningJobs_;
}

const std::vector<TPreemptedJob>& TSchedulingContextBase::PreemptedJobs() const
{
    return PreemptedJobs_;
}

void TSchedulingContextBase::StartJob(
    const TString& treeId,
    TOperationId operationId,
    TIncarnationId incarnationId,
    TControllerEpoch controllerEpoch,
    const TJobStartDescriptor& startDescriptor,
    EPreemptionMode preemptionMode,
    int schedulingIndex,
    EJobSchedulingStage schedulingStage)
{
    ResourceUsage_ += startDescriptor.ResourceLimits.ToJobResources();
    if (startDescriptor.ResourceLimits.DiskQuota()) {
        DiskRequests_.push_back(startDescriptor.ResourceLimits.DiskQuota());
    }
    auto startTime = NProfiling::CpuInstantToInstant(GetNow());
    auto job = New<TJob>(
        startDescriptor.Id,
        operationId,
        incarnationId,
        controllerEpoch,
        Node_,
        startTime,
        startDescriptor.ResourceLimits.ToJobResources(),
        startDescriptor.ResourceLimits.DiskQuota(),
        preemptionMode,
        treeId,
        schedulingIndex,
        schedulingStage);
    StartedJobs_.push_back(job);
}

void TSchedulingContextBase::PreemptJob(const TJobPtr& job, TDuration preemptionTimeout, EJobPreemptionReason preemptionReason)
{
    YT_VERIFY(job->GetNode() == Node_);
    PreemptedJobs_.push_back({job, preemptionTimeout, preemptionReason});
}

TJobResources TSchedulingContextBase::GetNodeFreeResourcesWithoutDiscount() const
{
    return ResourceLimits_ - ResourceUsage_;
}

TJobResources TSchedulingContextBase::GetNodeFreeResourcesWithDiscount() const
{
    return ResourceLimits_ - ResourceUsage_ + UnconditionalResourceUsageDiscount_;
}

TJobResources TSchedulingContextBase::GetNodeFreeResourcesWithDiscountForOperation(TOperationId operationId) const
{
    return ResourceLimits_ - ResourceUsage_ + UnconditionalResourceUsageDiscount_ + GetConditionalDiscountForOperation(operationId);
}

TScheduleJobsStatistics TSchedulingContextBase::GetSchedulingStatistics() const
{
    return SchedulingStatistics_;
}

void TSchedulingContextBase::SetSchedulingStatistics(TScheduleJobsStatistics statistics)
{
    SchedulingStatistics_ = statistics;
}

void TSchedulingContextBase::StoreScheduleJobExecDurationEstimate(TDuration duration)
{
    YT_ASSERT(!ScheduleJobExecDurationEstimate_);

    ScheduleJobExecDurationEstimate_ = duration;
}

TDuration TSchedulingContextBase::ExtractScheduleJobExecDurationEstimate()
{
    YT_ASSERT(ScheduleJobExecDurationEstimate_);

    return *std::exchange(ScheduleJobExecDurationEstimate_, {});
}

void TSchedulingContextBase::ResetUsageDiscounts()
{
    UnconditionalResourceUsageDiscount_ = {};
    ConditionalUsageDiscountMap_.clear();
    MaxConditionalUsageDiscount_ = {};
}

void TSchedulingContextBase::SetConditionalDiscountForOperation(TOperationId operationId, const TJobResources& discount)
{
    YT_VERIFY(ConditionalUsageDiscountMap_.emplace(operationId, discount).second);

    MaxConditionalUsageDiscount_ = Max(MaxConditionalUsageDiscount_, discount);
}

TJobResources TSchedulingContextBase::GetConditionalDiscountForOperation(TOperationId operationId) const
{
    auto it = ConditionalUsageDiscountMap_.find(operationId);
    return it != ConditionalUsageDiscountMap_.end() ? it->second : TJobResources{};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
