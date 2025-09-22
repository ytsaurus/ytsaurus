#include "scheduling_heartbeat_context_detail.h"

#include <yt/yt/server/scheduler/strategy/policy/public.h>

#include <yt/yt/server/scheduler/common/allocation.h>
#include <yt/yt/server/scheduler/common/exec_node.h>

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/scheduler/disk_resources.h>
#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/client/node_tracker_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NScheduler::NStrategy {

using namespace NPolicy;
using namespace NObjectClient;
using namespace NControllerAgent;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TSchedulingHeartbeatContextBase::TSchedulingHeartbeatContextBase(
    int nodeShardId,
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TAllocationPtr>& runningAllocations,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
    const TJobResources& defaultMinSpareAllocationResources)
    : NodeShardId_(nodeShardId)
    , Config_(std::move(config))
    , Node_(std::move(node))
    , NodeDescriptor_(Node_->BuildExecDescriptor())
    , NodeTags_(Node_->Tags())
    , MediumDirectory_(mediumDirectory)
    , DefaultMinSpareAllocationResources_(defaultMinSpareAllocationResources)
    , ResourceUsage_(Node_->ResourceUsage())
    , ResourceLimits_(Node_->ResourceLimits())
    , DiskResources_(Node_->DiskResources())
    , RunningAllocations_(runningAllocations)
{
    if (const auto& diskLocationResources = DiskResources_.DiskLocationResources;
        diskLocationResources.size() == 1 &&
        Config_->ConsiderDiskQuotaInPreemptiveSchedulingDiscount)
    {
        DiscountMediumIndex_ = diskLocationResources.front().MediumIndex;
    }
}

int TSchedulingHeartbeatContextBase::GetNodeShardId() const
{
    return NodeShardId_;
}

TJobResources& TSchedulingHeartbeatContextBase::ResourceUsage()
{
    return ResourceUsage_;
}

const TJobResources& TSchedulingHeartbeatContextBase::ResourceUsage() const
{
    return ResourceUsage_;
}

const TJobResources& TSchedulingHeartbeatContextBase::ResourceLimits() const
{
    return ResourceLimits_;
}

TJobResourcesWithQuota TSchedulingHeartbeatContextBase::GetDiscount() const
{
    return ToJobResourcesWithQuota(Discount_);
}

void TSchedulingHeartbeatContextBase::IncreaseDiscount(const TJobResourcesWithQuota& allocationResources)
{
    Discount_.JobResources += allocationResources.ToJobResources();
    Discount_.DiscountMediumDiskQuota += GetDiscountMediumQuota(allocationResources.DiskQuota());
}

const TDiskResources& TSchedulingHeartbeatContextBase::DiskResources() const
{
    return DiskResources_;
}

TDiskResources& TSchedulingHeartbeatContextBase::DiskResources()
{
    return DiskResources_;
}

const std::vector<TDiskQuota>& TSchedulingHeartbeatContextBase::DiskRequests() const
{
    return DiskRequests_;
}

const TExecNodeDescriptorPtr& TSchedulingHeartbeatContextBase::GetNodeDescriptor() const
{
    return NodeDescriptor_;
}

bool TSchedulingHeartbeatContextBase::CanSatisfyResourceRequest(
    const TJobResources& allocationResources,
    TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>* unsatisfiedResources) const
{
    auto resourceRequest = ResourceUsage_ + allocationResources - Discount_.JobResources;
    bool canSatisfyResourceRequest = true;

    #define XX(name, Name) \
        if (ResourceLimits_.Get##Name() < resourceRequest.Get##Name()) { \
            canSatisfyResourceRequest = false; \
            if (unsatisfiedResources) { \
                (*unsatisfiedResources)[EJobResourceWithDiskQuotaType::Name] = true; \
            } \
        }
    ITERATE_JOB_RESOURCES(XX)
    #undef XX

    return canSatisfyResourceRequest;
}

bool TSchedulingHeartbeatContextBase::CanStartAllocation(
    const TJobResourcesWithQuota& allocationResourcesWithQuota,
    TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>* unsatisfiedResources) const
{
    // TODO(eshcherbin): Remove |IsFullHostGpuAllocation| and make this decision in scheduling policy.
    // NB(omgronny): We ignore disk usage in case of full host GPU allocation.
    auto considerDiskUsage = !IsFullHostGpuAllocation(allocationResourcesWithQuota.ToJobResources());

    auto diskRequest = allocationResourcesWithQuota.DiskQuota();
    if (DiscountMediumIndex_ && considerDiskUsage) {
        if (DiskResources_.DefaultMediumIndex == *DiscountMediumIndex_ && diskRequest.DiskSpaceWithoutMedium) {
            diskRequest.DiskSpacePerMedium[*DiscountMediumIndex_] += *diskRequest.DiskSpaceWithoutMedium;
            diskRequest.DiskSpaceWithoutMedium.reset();
        }

        i64 totalDiskQuotaDiscount = Discount_.DiscountMediumDiskQuota;
        diskRequest.DiskSpacePerMedium[*DiscountMediumIndex_] = std::max(diskRequest.DiskSpacePerMedium[*DiscountMediumIndex_] - totalDiskQuotaDiscount, 0l);
    }

    std::vector<TDiskQuota> diskRequests(DiskRequests_);
    diskRequests.push_back(std::move(diskRequest));

    bool canSatisfyResourceRequest = CanSatisfyResourceRequest(
        allocationResourcesWithQuota.ToJobResources(),
        unsatisfiedResources);

    bool canSatisfyDiskQuotaRequests = CanSatisfyDiskQuotaRequests(
        DiskResources_,
        diskRequests,
        considerDiskUsage);

    (*unsatisfiedResources)[EJobResourceWithDiskQuotaType::DiskQuota] |= !canSatisfyDiskQuotaRequests;

    return canSatisfyResourceRequest && canSatisfyDiskQuotaRequests;
}

bool TSchedulingHeartbeatContextBase::CanStartMoreAllocations(
    const std::optional<TJobResources>& customMinSpareAllocationResources) const
{
    auto minSpareAllocationResources = customMinSpareAllocationResources.value_or(DefaultMinSpareAllocationResources_);
    if (!CanSatisfyResourceRequest(minSpareAllocationResources)) {
        return false;
    }

    auto limit = Config_->MaxStartedAllocationsPerHeartbeat;
    return !limit || std::ssize(StartedAllocations_) < *limit;
}

bool TSchedulingHeartbeatContextBase::CanSchedule(const TSchedulingTagFilter& filter) const
{
    return filter.IsEmpty() || filter.CanSchedule(NodeTags_);
}

bool TSchedulingHeartbeatContextBase::ShouldAbortAllocationsSinceResourcesOvercommit() const
{
    bool resourcesOvercommitted = !Dominates(ResourceLimits(), ResourceUsage());
    auto now = NProfiling::CpuInstantToInstant(GetNow());
    bool allowedOvercommitTimePassed = Node_->GetResourcesOvercommitStartTime()
        ? Node_->GetResourcesOvercommitStartTime() + Config_->AllowedNodeResourcesOvercommitDuration < now
        : false;
    return resourcesOvercommitted && allowedOvercommitTimePassed;
}

const std::vector<TStartedAllocation>& TSchedulingHeartbeatContextBase::StartedAllocations() const
{
    return StartedAllocations_;
}

const std::vector<TAllocationPtr>& TSchedulingHeartbeatContextBase::RunningAllocations() const
{
    return RunningAllocations_;
}

const std::vector<TPreemptedAllocation>& TSchedulingHeartbeatContextBase::PreemptedAllocations() const
{
    return PreemptedAllocations_;
}

void TSchedulingHeartbeatContextBase::StartAllocation(
    const TString& treeId,
    TOperationId operationId,
    TIncarnationId incarnationId,
    TControllerEpoch controllerEpoch,
    const TAllocationStartDescriptor& startDescriptor,
    EPreemptionMode preemptionMode,
    int schedulingIndex,
    EAllocationSchedulingStage schedulingStage,
    std::optional<TNetworkPriority> networkPriority)
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
        startDescriptor,
        preemptionMode,
        treeId,
        schedulingIndex,
        networkPriority);

    StartedAllocations_.push_back(TStartedAllocation{
        .Allocation = std::move(allocation),
        .SchedulingStage = schedulingStage,
    });
}

void TSchedulingHeartbeatContextBase::PreemptAllocation(const TAllocationPtr& allocation, TDuration preemptionTimeout, EAllocationPreemptionReason preemptionReason)
{
    YT_VERIFY(allocation->GetNode() == Node_);
    PreemptedAllocations_.push_back({allocation, preemptionTimeout, preemptionReason});

    if (auto it = DiskRequestIndexPerAllocationId_.find(allocation->GetId());
        it != DiskRequestIndexPerAllocationId_.end() && DiscountMediumIndex_)
    {
        DiskRequests_[it->second] = TDiskQuota{};
    }
}

TJobResources TSchedulingHeartbeatContextBase::GetNodeFreeResourcesWithoutDiscount() const
{
    return ResourceLimits_ - ResourceUsage_;
}

TJobResources TSchedulingHeartbeatContextBase::GetNodeFreeResourcesWithDiscount() const
{
    return ResourceLimits_ - ResourceUsage_ + Discount_.JobResources;
}

TDiskResources TSchedulingHeartbeatContextBase::GetNodeFreeDiskResourcesWithDiscount(const TJobResources& allocationResources) const
{
    auto diskResources = DiskResources_;

    if (IsFullHostGpuAllocation(allocationResources)) {
        for (auto& diskLocation : diskResources.DiskLocationResources) {
            diskLocation.Usage = 0;
        }
    } else if (DiscountMediumIndex_) {
        auto& diskLocation = diskResources.DiskLocationResources.front();
        diskLocation.Usage = std::max(0l, diskLocation.Usage - Discount_.DiscountMediumDiskQuota);
    }

    return diskResources;
}

TScheduleAllocationsStatistics TSchedulingHeartbeatContextBase::GetSchedulingStatistics() const
{
    return SchedulingStatistics_;
}

void TSchedulingHeartbeatContextBase::SetSchedulingStatistics(TScheduleAllocationsStatistics statistics)
{
    SchedulingStatistics_ = statistics;
}

void TSchedulingHeartbeatContextBase::StoreScheduleAllocationExecDurationEstimate(TDuration duration)
{
    YT_ASSERT(!ScheduleAllocationExecDurationEstimate_);

    ScheduleAllocationExecDurationEstimate_ = duration;
}

TDuration TSchedulingHeartbeatContextBase::ExtractScheduleAllocationExecDurationEstimate()
{
    YT_ASSERT(ScheduleAllocationExecDurationEstimate_);

    return *std::exchange(ScheduleAllocationExecDurationEstimate_, {});
}

bool TSchedulingHeartbeatContextBase::IsHeartbeatTimeoutExpired() const
{
    return HeartbeatTimeoutExpired_;
}

void TSchedulingHeartbeatContextBase::SetHeartbeatTimeoutExpired()
{
    HeartbeatTimeoutExpired_ = true;
}

void TSchedulingHeartbeatContextBase::ResetDiscount()
{
    Discount_ = {};
}

TJobResourcesWithQuota TSchedulingHeartbeatContextBase::ToJobResourcesWithQuota(const TJobResourcesWithQuotaDiscount& resources) const
{
    return TJobResourcesWithQuota(resources.JobResources, ToDiscountDiskQuota(resources.DiscountMediumDiskQuota));
}

TDiskQuota TSchedulingHeartbeatContextBase::ToDiscountDiskQuota(std::optional<i64> discountMediumQuota) const
{
    TDiskQuota diskQuota;
    if (DiscountMediumIndex_ && discountMediumQuota) {
        diskQuota.DiskSpacePerMedium.emplace(*DiscountMediumIndex_, *discountMediumQuota);
    }
    return diskQuota;
}

i64 TSchedulingHeartbeatContextBase::GetDiscountMediumQuota(const TDiskQuota& diskQuota) const
{
    if (!DiscountMediumIndex_) {
        return 0;
    }

    i64 quota = GetOrDefault(diskQuota.DiskSpacePerMedium, *DiscountMediumIndex_);
    if (DiskResources_.DefaultMediumIndex == *DiscountMediumIndex_) {
        quota += diskQuota.DiskSpaceWithoutMedium.value_or(0);
    }

    return quota;
}

bool TSchedulingHeartbeatContextBase::IsFullHostGpuAllocation(const TJobResources& allocationResources)
{
    return allocationResources.GetGpu() == NPolicy::FullHostGpuAllocationGpuDemand;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
