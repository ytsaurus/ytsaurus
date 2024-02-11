#include "scheduling_context_detail.h"

#include "exec_node.h"
#include "allocation.h"
#include "private.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/scheduler/disk_resources.h>
#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/client/node_tracker_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NScheduler {

using namespace NObjectClient;
using namespace NControllerAgent;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TSchedulingContextBase::TSchedulingContextBase(
    int nodeShardId,
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TAllocationPtr>& runningAllocations,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
    : NodeShardId_(nodeShardId)
    , Config_(std::move(config))
    , Node_(std::move(node))
    , NodeDescriptor_(Node_->BuildExecDescriptor())
    , NodeTags_(Node_->Tags())
    , MediumDirectory_(mediumDirectory)
    , DefaultMinSpareAllocationResources_(
        Config_->MinSpareAllocationResourcesOnNode
        ? ToJobResources(*Config_->MinSpareAllocationResourcesOnNode, TJobResources())
        : TJobResources())
    , ResourceUsage_(Node_->GetResourceUsage())
    , ResourceLimits_(Node_->GetResourceLimits())
    , DiskResources_(Node_->GetDiskResources())
    , RunningAllocations_(runningAllocations)
{
    if (const auto& diskLocationResources = DiskResources_.DiskLocationResources;
        diskLocationResources.size() == 1 &&
        Config_->ConsiderDiskQuotaInPreemptiveSchedulingDiscount)
    {
        DiscountMediumIndex_ = diskLocationResources.front().MediumIndex;
    }
}

int TSchedulingContextBase::GetNodeShardId() const
{
    return NodeShardId_;
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

const TJobResourcesWithQuota& TSchedulingContextBase::UnconditionalDiscount() const
{
    return UnconditionalDiscount_;
}

TJobResourcesWithQuota TSchedulingContextBase::GetConditionalDiscountForOperation(TOperationId operationId) const
{
    return GetOrDefault(ConditionalDiscountMap_, operationId);
}

TJobResourcesWithQuota TSchedulingContextBase::GetMaxConditionalDiscount() const
{
    return MaxConditionalDiscount_;
}

void TSchedulingContextBase::IncreaseUnconditionalDiscount(const TJobResourcesWithQuota& allocationResources)
{
    UnconditionalDiscount_.SetJobResources(UnconditionalDiscount_.ToJobResources() + allocationResources.ToJobResources());

    if (DiscountMediumIndex_) {
        auto compactedDiskQuota = GetDiskQuotaWithCompactedDefaultMedium(allocationResources.DiskQuota());
        UnconditionalDiscount_.DiskQuota().DiskSpacePerMedium[*DiscountMediumIndex_] +=
            GetOrDefault(compactedDiskQuota.DiskSpacePerMedium, *DiscountMediumIndex_);
    }
}

const TDiskResources& TSchedulingContextBase::DiskResources() const
{
    return DiskResources_;
}

TDiskResources& TSchedulingContextBase::DiskResources()
{
    return DiskResources_;
}

const std::vector<TDiskQuota>& TSchedulingContextBase::DiskRequests() const
{
    return DiskRequests_;
}

const TExecNodeDescriptorPtr& TSchedulingContextBase::GetNodeDescriptor() const
{
    return NodeDescriptor_;
}

bool TSchedulingContextBase::CanSatisfyResourceRequest(
    const TJobResources& allocationResources,
    const TJobResources& conditionalDiscount) const
{
    return Dominates(
        ResourceLimits_,
        ResourceUsage_ + allocationResources - (UnconditionalDiscount_.ToJobResources() + conditionalDiscount));
}

bool TSchedulingContextBase::CanStartAllocationForOperation(
    const TJobResourcesWithQuota& allocationResourcesWithQuota,
    TOperationId operationId) const
{
    std::vector<NScheduler::TDiskQuota> diskRequests(DiskRequests_);
    auto diskRequest = Max(TDiskQuota{},
        GetDiskQuotaWithCompactedDefaultMedium(allocationResourcesWithQuota.DiskQuota()) - (UnconditionalDiscount_.DiskQuota() + GetConditionalDiscountForOperation(operationId).DiskQuota()));
    diskRequests.push_back(std::move(diskRequest));

    return
        CanSatisfyResourceRequest(
            allocationResourcesWithQuota.ToJobResources(),
            GetConditionalDiscountForOperation(operationId).ToJobResources()) &&
        CanSatisfyDiskQuotaRequests(DiskResources_, diskRequests);
}

bool TSchedulingContextBase::CanStartMoreAllocations(
    const std::optional<TJobResources>& customMinSpareAllocationResources) const
{
    auto minSpareAllocationResources = customMinSpareAllocationResources.value_or(DefaultMinSpareAllocationResources_);
    if (!CanSatisfyResourceRequest(minSpareAllocationResources, MaxConditionalDiscount_.ToJobResources())) {
        return false;
    }

    auto limit = Config_->MaxStartedAllocationsPerHeartbeat;
    return !limit || std::ssize(StartedAllocations_) < *limit;
}

bool TSchedulingContextBase::CanSchedule(const TSchedulingTagFilter& filter) const
{
    return filter.IsEmpty() || filter.CanSchedule(NodeTags_);
}

bool TSchedulingContextBase::ShouldAbortAllocationsSinceResourcesOvercommit() const
{
    bool resourcesOvercommitted = !Dominates(ResourceLimits(), ResourceUsage());
    auto now = NProfiling::CpuInstantToInstant(GetNow());
    bool allowedOvercommitTimePassed = Node_->GetResourcesOvercommitStartTime()
        ? Node_->GetResourcesOvercommitStartTime() + Config_->AllowedNodeResourcesOvercommitDuration < now
        : false;
    return resourcesOvercommitted && allowedOvercommitTimePassed;
}

const std::vector<TAllocationPtr>& TSchedulingContextBase::StartedAllocations() const
{
    return StartedAllocations_;
}

const std::vector<TAllocationPtr>& TSchedulingContextBase::RunningAllocations() const
{
    return RunningAllocations_;
}

const std::vector<TPreemptedAllocation>& TSchedulingContextBase::PreemptedAllocations() const
{
    return PreemptedAllocations_;
}

void TSchedulingContextBase::StartAllocation(
    const TString& treeId,
    TOperationId operationId,
    TIncarnationId incarnationId,
    TControllerEpoch controllerEpoch,
    const TAllocationStartDescriptor& startDescriptor,
    EPreemptionMode preemptionMode,
    int schedulingIndex,
    EAllocationSchedulingStage schedulingStage)
{
    ResourceUsage_ += startDescriptor.ResourceLimits.ToJobResources();
    if (startDescriptor.ResourceLimits.DiskQuota()) {
        DiskRequests_.push_back(startDescriptor.ResourceLimits.DiskQuota());
    }
    auto startTime = NProfiling::CpuInstantToInstant(GetNow());
    auto allocation = New<TAllocation>(
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
    StartedAllocations_.push_back(std::move(allocation));
}

void TSchedulingContextBase::PreemptAllocation(const TAllocationPtr& allocation, TDuration preemptionTimeout, EAllocationPreemptionReason preemptionReason)
{
    YT_VERIFY(allocation->GetNode() == Node_);
    PreemptedAllocations_.push_back({allocation, preemptionTimeout, preemptionReason});

    if (auto it = DiskRequestIndexPerAllocationId_.find(allocation->GetId());
        it != DiskRequestIndexPerAllocationId_.end() && DiscountMediumIndex_)
    {
        DiskRequests_[it->second] = TDiskQuota{};
    }
}

TJobResources TSchedulingContextBase::GetNodeFreeResourcesWithoutDiscount() const
{
    return ResourceLimits_ - ResourceUsage_;
}

TJobResources TSchedulingContextBase::GetNodeFreeResourcesWithDiscount() const
{
    return ResourceLimits_ - ResourceUsage_ + UnconditionalDiscount_.ToJobResources();
}

TJobResources TSchedulingContextBase::GetNodeFreeResourcesWithDiscountForOperation(TOperationId operationId) const
{
    return ResourceLimits_ - ResourceUsage_ + UnconditionalDiscount_.ToJobResources() + GetConditionalDiscountForOperation(operationId).ToJobResources();
}

TDiskResources TSchedulingContextBase::GetDiskResourcesWithDiscountForOperation(TOperationId operationId) const
{
    auto diskResources = DiskResources_;
    if (DiscountMediumIndex_) {
        auto discountForOperation = GetOrDefault(UnconditionalDiscount_.DiskQuota().DiskSpacePerMedium, *DiscountMediumIndex_) +
            GetOrDefault(GetConditionalDiscountForOperation(operationId).DiskQuota().DiskSpacePerMedium, *DiscountMediumIndex_);

        auto& diskLocation = diskResources.DiskLocationResources.front();
        diskLocation.Usage = std::max(0l, diskLocation.Usage - discountForOperation);
    }
    return diskResources;
}

TScheduleAllocationsStatistics TSchedulingContextBase::GetSchedulingStatistics() const
{
    return SchedulingStatistics_;
}

void TSchedulingContextBase::SetSchedulingStatistics(TScheduleAllocationsStatistics statistics)
{
    SchedulingStatistics_ = statistics;
}

void TSchedulingContextBase::StoreScheduleAllocationExecDurationEstimate(TDuration duration)
{
    YT_ASSERT(!ScheduleAllocationExecDurationEstimate_);

    ScheduleAllocationExecDurationEstimate_ = duration;
}

TDuration TSchedulingContextBase::ExtractScheduleAllocationExecDurationEstimate()
{
    YT_ASSERT(ScheduleAllocationExecDurationEstimate_);

    return *std::exchange(ScheduleAllocationExecDurationEstimate_, {});
}

void TSchedulingContextBase::ResetDiscounts()
{
    UnconditionalDiscount_ = {};
    ConditionalDiscountMap_.clear();
    MaxConditionalDiscount_ = {};
}

void TSchedulingContextBase::SetConditionalDiscountForOperation(TOperationId operationId, const TJobResourcesWithQuota& discountForOperation)
{
    TJobResourcesWithQuota conditionalDiscount(discountForOperation.ToJobResources());

    if (DiscountMediumIndex_) {
        auto compactedDiscount = GetDiskQuotaWithCompactedDefaultMedium(discountForOperation.DiskQuota());
        conditionalDiscount.DiskQuota().DiskSpacePerMedium[*DiscountMediumIndex_] =
            GetOrDefault(compactedDiscount.DiskSpacePerMedium, *DiscountMediumIndex_);
    }

    EmplaceOrCrash(ConditionalDiscountMap_, operationId, conditionalDiscount);
    MaxConditionalDiscount_ = Max(MaxConditionalDiscount_, conditionalDiscount);
}

TDiskQuota TSchedulingContextBase::GetDiskQuotaWithCompactedDefaultMedium(TDiskQuota diskQuota) const
{
    if (diskQuota.DiskSpaceWithoutMedium) {
        diskQuota.DiskSpacePerMedium[DiskResources_.DefaultMediumIndex] += *diskQuota.DiskSpaceWithoutMedium;
        diskQuota.DiskSpaceWithoutMedium = {};
    }

    return diskQuota;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
