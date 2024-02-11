#pragma once

#include "scheduling_context.h"
#include "exec_node.h"

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContextBase
    : public ISchedulingContext
{
public:
    TSchedulingContextBase(
        int nodeShardId,
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        const std::vector<TAllocationPtr>& runningAllocations,
        const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

    int GetNodeShardId() const override;

    void ResetDiscounts() override;

    const TJobResourcesWithQuota& UnconditionalDiscount() const override;
    void IncreaseUnconditionalDiscount(const TJobResourcesWithQuota& allocationResources) override;

    TJobResourcesWithQuota GetMaxConditionalDiscount() const override;
    TJobResourcesWithQuota GetConditionalDiscountForOperation(TOperationId operationId) const override;
    void SetConditionalDiscountForOperation(TOperationId operationId, const TJobResourcesWithQuota& discountForOperation) override;

    TDiskResources GetDiskResourcesWithDiscountForOperation(TOperationId operationId) const override;
    TJobResources GetNodeFreeResourcesWithoutDiscount() const override;
    TJobResources GetNodeFreeResourcesWithDiscount() const override;
    TJobResources GetNodeFreeResourcesWithDiscountForOperation(TOperationId operationId) const override;

    const TJobResources& ResourceLimits() const override;
    const TJobResources& ResourceUsage() const;
    TJobResources& ResourceUsage() override;

    const TDiskResources& DiskResources() const override;
    TDiskResources& DiskResources();
    const std::vector<TDiskQuota>& DiskRequests() const override;

    const TExecNodeDescriptorPtr& GetNodeDescriptor() const override;

    bool CanStartAllocationForOperation(
        const TJobResourcesWithQuota& allocationResourcesWithQuota,
        TOperationId operationId) const override;
    bool CanStartMoreAllocations(
        const std::optional<TJobResources>& customMinSpareAllocationResources) const override;
    bool CanSchedule(const TSchedulingTagFilter& filter) const override;
    bool ShouldAbortAllocationsSinceResourcesOvercommit() const override;

    const std::vector<TAllocationPtr>& StartedAllocations() const override;
    const std::vector<TAllocationPtr>& RunningAllocations() const override;
    const std::vector<TPreemptedAllocation>& PreemptedAllocations() const override;

    void StartAllocation(
        const TString& treeId,
        TOperationId operationId,
        TIncarnationId incarnationId,
        TControllerEpoch controllerEpoch,
        const TAllocationStartDescriptor& startDescriptor,
        EPreemptionMode preemptionMode,
        int schedulingIndex,
        EAllocationSchedulingStage schedulingStage) override;

    void PreemptAllocation(const TAllocationPtr& allocation, TDuration preemptionTimeout, EAllocationPreemptionReason preemptionReason) override;

    TScheduleAllocationsStatistics GetSchedulingStatistics() const override;
    void SetSchedulingStatistics(TScheduleAllocationsStatistics statistics) override;

    virtual void StoreScheduleAllocationExecDurationEstimate(TDuration duration) override;
    virtual TDuration ExtractScheduleAllocationExecDurationEstimate() override;

private:
    const int NodeShardId_;
    const TSchedulerConfigPtr Config_;
    const TExecNodePtr Node_;
    const TExecNodeDescriptorPtr NodeDescriptor_;
    const TBooleanFormulaTags NodeTags_;
    const NChunkClient::TMediumDirectoryPtr MediumDirectory_;
    const TJobResources DefaultMinSpareAllocationResources_;

    TJobResourcesWithQuota UnconditionalDiscount_;
    TJobResourcesWithQuota MaxConditionalDiscount_;

    TJobResources ResourceUsage_;
    TJobResources ResourceLimits_;

    TDiskResources DiskResources_;
    std::optional<int> DiscountMediumIndex_;

    std::vector<TAllocationPtr> StartedAllocations_;
    std::vector<TAllocationPtr> RunningAllocations_;
    std::vector<TPreemptedAllocation> PreemptedAllocations_;

    std::vector<TDiskQuota> DiskRequests_;
    THashMap<TAllocationId, int> DiskRequestIndexPerAllocationId_;

    // TODO(eshcherbin): Should we optimize and use tree index instead of operation ID here?
    THashMap<TOperationId, TJobResourcesWithQuota> ConditionalDiscountMap_;

    TScheduleAllocationsStatistics SchedulingStatistics_;

    std::optional<TDuration> ScheduleAllocationExecDurationEstimate_;

    bool CanSatisfyResourceRequest(
        const TJobResources& allocationResources,
        const TJobResources& conditionalDiscount) const;

    TDiskQuota GetDiskQuotaWithCompactedDefaultMedium(TDiskQuota diskQuota) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
