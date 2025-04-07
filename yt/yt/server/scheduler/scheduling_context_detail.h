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

    void InitializeConditionalDiscounts(int capacity) override;
    void ResetDiscounts() override;

    TJobResourcesWithQuota GetUnconditionalDiscount() const override;
    void IncreaseUnconditionalDiscount(const TJobResourcesWithQuota& allocationResources) override;

    TJobResourcesWithQuota GetMaxConditionalDiscount() const override;
    TJobResourcesWithQuota GetConditionalDiscountForOperation(TOperationIndex operationIndex) const override;
    void SetConditionalDiscountForOperation(TOperationIndex operationIndex, const TJobResourcesWithQuota& discountForOperation) override;

    TDiskResources GetDiskResourcesWithDiscountForOperation(TOperationIndex operationIndex, const TJobResources& allocationResources) const override;
    TJobResources GetNodeFreeResourcesWithoutDiscount() const override;
    TJobResources GetNodeFreeResourcesWithDiscount() const override;
    TJobResources GetNodeFreeResourcesWithDiscountForOperation(TOperationIndex operationIndex) const override;

    const TJobResources& ResourceLimits() const override;
    const TJobResources& ResourceUsage() const;
    TJobResources& ResourceUsage() override;

    const TDiskResources& DiskResources() const override;
    TDiskResources& DiskResources();
    const std::vector<TDiskQuota>& DiskRequests() const override;

    const TExecNodeDescriptorPtr& GetNodeDescriptor() const override;

    bool CanStartAllocationForOperation(
        const TJobResourcesWithQuota& allocationResourcesWithQuota,
        TOperationIndex operationIndex,
        TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>* unsatisfiedResources) const override;
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

    void StoreScheduleAllocationExecDurationEstimate(TDuration duration) override;
    TDuration ExtractScheduleAllocationExecDurationEstimate() override;

    ESchedulingStopReason GetSchedulingStopReason() const override;
    void SetSchedulingStopReason(ESchedulingStopReason result) override;

private:
    const int NodeShardId_;
    const TSchedulerConfigPtr Config_;
    const TExecNodePtr Node_;
    const TExecNodeDescriptorPtr NodeDescriptor_;
    const TBooleanFormulaTags NodeTags_;
    const NChunkClient::TMediumDirectoryPtr MediumDirectory_;
    const TJobResources DefaultMinSpareAllocationResources_;

    TJobResources ResourceUsage_;
    TJobResources ResourceLimits_;

    TDiskResources DiskResources_;
    std::vector<TDiskQuota> DiskRequests_;
    THashMap<TAllocationId, int> DiskRequestIndexPerAllocationId_;

    std::vector<TAllocationPtr> StartedAllocations_;
    std::vector<TAllocationPtr> RunningAllocations_;
    std::vector<TPreemptedAllocation> PreemptedAllocations_;

    struct TJobResourcesWithQuotaDiscount
    {
        TJobResources JobResources;
        i64 DiscountMediumDiskQuota = 0;
    };

    std::optional<int> DiscountMediumIndex_;
    TJobResourcesWithQuotaDiscount UnconditionalDiscount_;
    TJobResourcesWithQuotaDiscount MaxConditionalDiscount_;
    std::vector<TJobResourcesWithQuotaDiscount> ConditionalDiscounts_;

    TScheduleAllocationsStatistics SchedulingStatistics_;

    std::optional<TDuration> ScheduleAllocationExecDurationEstimate_;

    ESchedulingStopReason SchedulingStopReason_ = ESchedulingStopReason::FullyScheduled;

    // NB(omgronny): Don't collect unsatisfied resources info if unsatisfiedResources is nullptr.
    bool CanSatisfyResourceRequest(
        const TJobResources& allocationResources,
        const TJobResources& conditionalDiscount,
        TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>* unsatisfiedResources = nullptr) const;

    const TJobResourcesWithQuotaDiscount& ConditionalDiscountForOperation(TOperationIndex operationIndex) const;

    TJobResourcesWithQuota ToJobResourcesWithQuota(const TJobResourcesWithQuotaDiscount& resources) const;
    TDiskQuota ToDiscountDiskQuota(std::optional<i64> discountMediumQuota) const;
    i64 GetDiscountMediumQuota(const TDiskQuota& diskQuota) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
