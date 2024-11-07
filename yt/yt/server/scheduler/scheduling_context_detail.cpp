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

TJobResourcesWithQuota TSchedulingContextBase::GetUnconditionalDiscount() const
{
    return ToJobResourcesWithQuota(UnconditionalDiscount_);
}

const TSchedulingContextBase::TJobResourcesWithQuotaDiscount& TSchedulingContextBase::ConditionalDiscountForOperation(TOperationIndex operationIndex) const
{
    // NB(eshcherbin): |VectorAtOr| returns |const T&|, so we must provide an explicit default value not to shoot ourself.
    static constexpr TJobResourcesWithQuotaDiscount ZeroDiscount;
    return VectorAtOr(ConditionalDiscounts_, operationIndex, ZeroDiscount);
}

TJobResourcesWithQuota TSchedulingContextBase::GetConditionalDiscountForOperation(TOperationIndex operationIndex) const
{
    return ToJobResourcesWithQuota(ConditionalDiscountForOperation(operationIndex));
}

TJobResourcesWithQuota TSchedulingContextBase::GetMaxConditionalDiscount() const
{
    return ToJobResourcesWithQuota(MaxConditionalDiscount_);
}

void TSchedulingContextBase::IncreaseUnconditionalDiscount(const TJobResourcesWithQuota& allocationResources)
{
    UnconditionalDiscount_.JobResources += allocationResources.ToJobResources();
    UnconditionalDiscount_.DiscountMediumDiskQuota += GetDiscountMediumQuota(allocationResources.DiskQuota());
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
    const TJobResources& conditionalDiscount,
    TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>* unsatisfiedResources) const
{
    auto resourceRequest = ResourceUsage_ + allocationResources - (UnconditionalDiscount_.JobResources + conditionalDiscount);
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

bool TSchedulingContextBase::CanStartAllocationForOperation(
    const TJobResourcesWithQuota& allocationResourcesWithQuota,
    TOperationIndex operationIndex,
    TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>* unsatisfiedResources) const
{
    // NB(omgronny): We ignore disk usage in case of full host GPU allocation.
    auto considerUsage = !IsFullHostGpuAllocation(allocationResourcesWithQuota.ToJobResources());

    auto diskRequest = allocationResourcesWithQuota.DiskQuota();
    if (DiscountMediumIndex_ && considerUsage) {
        if (DiskResources_.DefaultMediumIndex == *DiscountMediumIndex_ && diskRequest.DiskSpaceWithoutMedium) {
            diskRequest.DiskSpacePerMedium[*DiscountMediumIndex_] += *diskRequest.DiskSpaceWithoutMedium;
            diskRequest.DiskSpaceWithoutMedium.reset();
        }

        i64 totalDiskQuotaDiscount = UnconditionalDiscount_.DiscountMediumDiskQuota + ConditionalDiscountForOperation(operationIndex).DiscountMediumDiskQuota;
        diskRequest.DiskSpacePerMedium[*DiscountMediumIndex_] = std::max(diskRequest.DiskSpacePerMedium[*DiscountMediumIndex_] - totalDiskQuotaDiscount, 0l);
    }

    std::vector<TDiskQuota> diskRequests(DiskRequests_);
    diskRequests.push_back(std::move(diskRequest));

    bool canSatisfyResourceRequest = CanSatisfyResourceRequest(
        allocationResourcesWithQuota.ToJobResources(),
        ConditionalDiscountForOperation(operationIndex).JobResources,
        unsatisfiedResources);

    bool canSatisfyDiskQuotaRequests = CanSatisfyDiskQuotaRequests(
        DiskResources_,
        diskRequests,
        considerUsage);

    (*unsatisfiedResources)[EJobResourceWithDiskQuotaType::DiskQuota] |= !canSatisfyDiskQuotaRequests;

    return canSatisfyResourceRequest && canSatisfyDiskQuotaRequests;
}

bool TSchedulingContextBase::CanStartMoreAllocations(
    const std::optional<TJobResources>& customMinSpareAllocationResources) const
{
    auto minSpareAllocationResources = customMinSpareAllocationResources.value_or(DefaultMinSpareAllocationResources_);
    if (!CanSatisfyResourceRequest(minSpareAllocationResources, MaxConditionalDiscount_.JobResources)) {
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
        startDescriptor,
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
    return ResourceLimits_ - ResourceUsage_ + UnconditionalDiscount_.JobResources;
}

TJobResources TSchedulingContextBase::GetNodeFreeResourcesWithDiscountForOperation(TOperationIndex operationIndex) const
{
    return ResourceLimits_ - ResourceUsage_ + UnconditionalDiscount_.JobResources + ConditionalDiscountForOperation(operationIndex).JobResources;
}

TDiskResources TSchedulingContextBase::GetDiskResourcesWithDiscountForOperation(TOperationIndex operationIndex) const
{
    auto diskResources = DiskResources_;
    if (DiscountMediumIndex_) {
        auto discountForOperation = UnconditionalDiscount_.DiscountMediumDiskQuota + ConditionalDiscountForOperation(operationIndex).DiscountMediumDiskQuota;

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

ESchedulingStopReason TSchedulingContextBase::GetSchedulingStopReason() const
{
    return SchedulingStopReason_;
}

void TSchedulingContextBase::SetSchedulingStopReason(ESchedulingStopReason result)
{
    SchedulingStopReason_ = result;
}

void TSchedulingContextBase::InitializeConditionalDiscounts(int capacity)
{
    ConditionalDiscounts_.reserve(capacity);
}

void TSchedulingContextBase::ResetDiscounts()
{
    UnconditionalDiscount_ = {};
    ConditionalDiscounts_.clear();
    MaxConditionalDiscount_ = {};
}

void TSchedulingContextBase::SetConditionalDiscountForOperation(TOperationIndex operationIndex, const TJobResourcesWithQuota& discountForOperation)
{
    TJobResourcesWithQuotaDiscount conditionalDiscount(discountForOperation.ToJobResources(), GetDiscountMediumQuota(discountForOperation.DiskQuota()));
    AssignVectorAt(ConditionalDiscounts_, operationIndex, conditionalDiscount);

    MaxConditionalDiscount_.JobResources = Max(MaxConditionalDiscount_.JobResources, conditionalDiscount.JobResources);
    MaxConditionalDiscount_.DiscountMediumDiskQuota = std::max(MaxConditionalDiscount_.DiscountMediumDiskQuota, conditionalDiscount.DiscountMediumDiskQuota);
}

TJobResourcesWithQuota TSchedulingContextBase::ToJobResourcesWithQuota(const TJobResourcesWithQuotaDiscount& resources) const
{
    return TJobResourcesWithQuota(resources.JobResources, ToDiscountDiskQuota(resources.DiscountMediumDiskQuota));
}

TDiskQuota TSchedulingContextBase::ToDiscountDiskQuota(std::optional<i64> discountMediumQuota) const
{
    TDiskQuota diskQuota;
    if (DiscountMediumIndex_ && discountMediumQuota) {
        diskQuota.DiskSpacePerMedium.emplace(*DiscountMediumIndex_, *discountMediumQuota);
    }
    return diskQuota;
}

i64 TSchedulingContextBase::GetDiscountMediumQuota(const TDiskQuota& diskQuota) const
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
