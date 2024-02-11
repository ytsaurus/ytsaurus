#pragma once

#include "private.h"

#include <yt/yt/server/lib/scheduler/scheduling_tag.h>

#include <yt/yt/server/lib/controller_agent/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/disk_resources.h>
#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Refactor TScheduleAllocationsStatistics to allow for different scheduling strategies.
struct TScheduleAllocationsStatistics
{
    int ControllerScheduleAllocationCount = 0;
    int ControllerScheduleAllocationTimedOutCount = 0;
    TEnumIndexedArray<EAllocationSchedulingStage, int> ScheduleAllocationAttemptCountPerStage;
    int MaxNonPreemptiveSchedulingIndex = -1;
    int ScheduledDuringPreemption = 0;
    int UnconditionallyPreemptibleAllocationCount = 0;
    int TotalConditionallyPreemptibleAllocationCount = 0;
    int MaxConditionallyPreemptibleAllocationCountInPool = 0;
    bool ScheduleWithPreemption = false;
    TEnumIndexedArray<EOperationPreemptionPriority, int> OperationCountByPreemptionPriority;
    TJobResources ResourceLimits;
    TJobResources ResourceUsage;
    TJobResources UnconditionalResourceUsageDiscount;
    TJobResources MaxConditionalResourceUsageDiscount;
    bool SsdPriorityPreemptionEnabled = false;
    THashSet<int> SsdPriorityPreemptionMedia;
};

void Serialize(const TScheduleAllocationsStatistics& statistics, NYson::IYsonConsumer* consumer);

TString FormatPreemptibleInfoCompact(const TScheduleAllocationsStatistics& statistics);
TString FormatScheduleAllocationAttemptsCompact(const TScheduleAllocationsStatistics& statistics);

TString FormatOperationCountByPreemptionPriorityCompact(
    const TEnumIndexedArray<EOperationPreemptionPriority, int>& operationsPerPriority);

////////////////////////////////////////////////////////////////////////////////

struct TPreemptedAllocation
{
    TAllocationPtr Allocation;
    TDuration PreemptionTimeout;
    EAllocationPreemptionReason PreemptionReason;
};

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingContext
    : public virtual TRefCounted
{
    virtual int GetNodeShardId() const = 0;

    virtual const TExecNodeDescriptorPtr& GetNodeDescriptor() const = 0;

    virtual const TJobResources& ResourceLimits() const = 0;
    virtual TJobResources& ResourceUsage() = 0;
    virtual const TDiskResources& DiskResources() const = 0;
    virtual const std::vector<TDiskQuota>& DiskRequests() const = 0;

    //! Used during preemption to allow second-chance scheduling.
    virtual void ResetDiscounts() = 0;
    virtual const TJobResourcesWithQuota& UnconditionalDiscount() const = 0;
    virtual void IncreaseUnconditionalDiscount(const TJobResourcesWithQuota& allocationResources) = 0;
    virtual TJobResourcesWithQuota GetMaxConditionalDiscount() const = 0;
    virtual TJobResourcesWithQuota GetConditionalDiscountForOperation(TOperationId operationId) const = 0;
    virtual void SetConditionalDiscountForOperation(TOperationId operationId, const TJobResourcesWithQuota& discount) = 0;
    virtual TDiskResources GetDiskResourcesWithDiscountForOperation(TOperationId operationId) const = 0;
    virtual TJobResources GetNodeFreeResourcesWithoutDiscount() const = 0;
    virtual TJobResources GetNodeFreeResourcesWithDiscount() const = 0;
    virtual TJobResources GetNodeFreeResourcesWithDiscountForOperation(TOperationId operationId) const = 0;

    virtual const std::vector<TAllocationPtr>& StartedAllocations() const = 0;
    virtual const std::vector<TAllocationPtr>& RunningAllocations() const = 0;
    virtual const std::vector<TPreemptedAllocation>& PreemptedAllocations() const = 0;

    //! Returns |true| if node has enough resources to start allocation with given limits.
    virtual bool CanStartAllocationForOperation(const TJobResourcesWithQuota& allocationResources, TOperationId operationId) const = 0;
    //! Returns |true| if any more new allocations can be scheduled at this node.
    virtual bool CanStartMoreAllocations(
        const std::optional<TJobResources>& customMinSpareAllocationResources = {}) const = 0;
    //! Returns |true| if the node can handle allocations demanding a certain #tag.
    virtual bool CanSchedule(const TSchedulingTagFilter& filter) const = 0;

    //! Returns |true| if strategy should abort allocations since resources overcommit.
    virtual bool ShouldAbortAllocationsSinceResourcesOvercommit() const = 0;

    virtual void StartAllocation(
        const TString& treeId,
        TOperationId operationId,
        TIncarnationId incarnationId,
        TControllerEpoch controllerEpoch,
        const TAllocationStartDescriptor& startDescriptor,
        EPreemptionMode preemptionMode,
        int schedulingIndex,
        EAllocationSchedulingStage schedulingStage) = 0;

    virtual void PreemptAllocation(const TAllocationPtr& allocation, TDuration preemptionTimeout, EAllocationPreemptionReason preemptionReason) = 0;

    virtual NProfiling::TCpuInstant GetNow() const = 0;

    virtual TScheduleAllocationsStatistics GetSchedulingStatistics() const = 0;
    virtual void SetSchedulingStatistics(TScheduleAllocationsStatistics statistics) = 0;

    virtual void StoreScheduleAllocationExecDurationEstimate(TDuration duration) = 0;
    virtual TDuration ExtractScheduleAllocationExecDurationEstimate() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulingContext)

////////////////////////////////////////////////////////////////////////////////

ISchedulingContextPtr CreateSchedulingContext(
    int nodeShardId,
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TAllocationPtr>& runningAllocations,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
