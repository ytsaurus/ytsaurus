#pragma once

#include <yt/yt/server/scheduler/strategy/policy/scheduling_heartbeat_context.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

class TSchedulingHeartbeatContextBase
    : public NPolicy::ISchedulingHeartbeatContext
{
public:
    TSchedulingHeartbeatContextBase(
        int nodeShardId,
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        const std::vector<TAllocationPtr>& runningAllocations,
        const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
        const TJobResources& defaultMinSpareAllocationResources);

    int GetNodeShardId() const override;

    void ResetDiscount() override;

    TJobResourcesWithQuota GetDiscount() const override;
    void IncreaseDiscount(const TJobResourcesWithQuota& allocationResources) override;

    TJobResources GetNodeFreeResourcesWithoutDiscount() const override;

    TJobResources GetNodeFreeResourcesWithDiscount() const override;
    TDiskResources GetNodeFreeDiskResourcesWithDiscount(const TJobResources& allocationResources) const override;

    const TJobResources& ResourceLimits() const override;
    const TJobResources& ResourceUsage() const;
    TJobResources& ResourceUsage() override;

    const TDiskResources& DiskResources() const override;
    TDiskResources& DiskResources();
    const std::vector<TDiskQuota>& DiskRequests() const override;

    const TExecNodeDescriptorPtr& GetNodeDescriptor() const override;

    bool CanStartAllocation(
        const TJobResourcesWithQuota& allocationResourcesWithQuota,
        TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>* unsatisfiedResources) const override;
    bool CanStartMoreAllocations(
        const std::optional<TJobResources>& customMinSpareAllocationResources) const override;
    bool CanSchedule(const TSchedulingTagFilter& filter) const override;
    bool ShouldAbortAllocationsSinceResourcesOvercommit() const override;

    const std::vector<NPolicy::TStartedAllocation>& StartedAllocations() const override;
    const std::vector<TAllocationPtr>& RunningAllocations() const override;
    const std::vector<NPolicy::TPreemptedAllocation>& PreemptedAllocations() const override;

    void StartAllocation(
        const TString& treeId,
        TOperationId operationId,
        TIncarnationId incarnationId,
        TControllerEpoch controllerEpoch,
        const TAllocationStartDescriptor& startDescriptor,
        EPreemptionMode preemptionMode,
        int schedulingIndex,
        NPolicy::EAllocationSchedulingStage schedulingStage,
        std::optional<TNetworkPriority> networkPriority) override;

    void PreemptAllocation(const TAllocationPtr& allocation, TDuration preemptionTimeout, NPolicy::EAllocationPreemptionReason preemptionReason) override;

    NPolicy::TScheduleAllocationsStatistics GetSchedulingStatistics() const override;
    void SetSchedulingStatistics(NPolicy::TScheduleAllocationsStatistics statistics) override;

    void StoreScheduleAllocationExecDurationEstimate(TDuration duration) override;
    TDuration ExtractScheduleAllocationExecDurationEstimate() override;

    bool IsHeartbeatTimeoutExpired() const override;
    void SetHeartbeatTimeoutExpired() override;

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

    std::vector<NPolicy::TStartedAllocation> StartedAllocations_;
    std::vector<TAllocationPtr> RunningAllocations_;
    std::vector<NPolicy::TPreemptedAllocation> PreemptedAllocations_;

    struct TJobResourcesWithQuotaDiscount
    {
        TJobResources JobResources;
        i64 DiscountMediumDiskQuota = 0;
    };

    std::optional<int> DiscountMediumIndex_;
    TJobResourcesWithQuotaDiscount Discount_;

    NPolicy::TScheduleAllocationsStatistics SchedulingStatistics_;

    std::optional<TDuration> ScheduleAllocationExecDurationEstimate_;

    bool HeartbeatTimeoutExpired_ = false;

    // NB(omgronny): Don't collect unsatisfied resources info if unsatisfiedResources is nullptr.
    bool CanSatisfyResourceRequest(
        const TJobResources& allocationResources,
        TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>* unsatisfiedResources = nullptr) const;

    TJobResourcesWithQuota ToJobResourcesWithQuota(const TJobResourcesWithQuotaDiscount& resources) const;
    TDiskQuota ToDiscountDiskQuota(std::optional<i64> discountMediumQuota) const;
    i64 GetDiscountMediumQuota(const TDiskQuota& diskQuota) const;

    static bool IsFullHostGpuAllocation(const TJobResources& allocationResources);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
