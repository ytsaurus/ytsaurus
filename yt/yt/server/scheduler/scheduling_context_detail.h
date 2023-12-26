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
        const std::vector<TJobPtr>& runningJobs,
        const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

    int GetNodeShardId() const override;

    void ResetDiscounts() override;

    const TJobResourcesWithQuota& UnconditionalDiscount() const override;
    void IncreaseUnconditionalDiscount(const TJobResourcesWithQuota& jobResources) override;

    TJobResourcesWithQuota GetMaxConditionalDiscount() const override;
    TJobResourcesWithQuota GetConditionalDiscountForOperation(TOperationId operationId) const override;
    void SetConditionalDiscountForOperation(TOperationId operationId, const TJobResourcesWithQuota& discountForOperation) override;

    NNodeTrackerClient::NProto::TDiskResources GetDiskResourcesWithDiscountForOperation(TOperationId operationId) const override;
    TJobResources GetNodeFreeResourcesWithoutDiscount() const override;
    TJobResources GetNodeFreeResourcesWithDiscount() const override;
    TJobResources GetNodeFreeResourcesWithDiscountForOperation(TOperationId operationId) const override;

    const TJobResources& ResourceLimits() const override;
    const TJobResources& ResourceUsage() const;
    TJobResources& ResourceUsage() override;

    const NNodeTrackerClient::NProto::TDiskResources& DiskResources() const override;
    NNodeTrackerClient::NProto::TDiskResources& DiskResources();
    const std::vector<TDiskQuota>& DiskRequests() const override;

    const TExecNodeDescriptorPtr& GetNodeDescriptor() const override;

    bool CanStartJobForOperation(
        const TJobResourcesWithQuota& jobResourcesWithQuota,
        TOperationId operationId) const override;
    bool CanStartMoreJobs(
        const std::optional<TJobResources>& customMinSpareJobResources) const override;
    bool CanSchedule(const TSchedulingTagFilter& filter) const override;
    bool ShouldAbortJobsSinceResourcesOvercommit() const override;

    const std::vector<TJobPtr>& StartedJobs() const override;
    const std::vector<TJobPtr>& RunningJobs() const override;
    const std::vector<TPreemptedJob>& PreemptedJobs() const override;

    void StartJob(
        const TString& treeId,
        TOperationId operationId,
        TIncarnationId incarnationId,
        TControllerEpoch controllerEpoch,
        const TJobStartDescriptor& startDescriptor,
        EPreemptionMode preemptionMode,
        int schedulingIndex,
        EJobSchedulingStage schedulingStage) override;

    void PreemptJob(const TJobPtr& job, TDuration interruptTimeout, EJobPreemptionReason preemptionReason) override;

    TScheduleJobsStatistics GetSchedulingStatistics() const override;
    void SetSchedulingStatistics(TScheduleJobsStatistics statistics) override;

    virtual void StoreScheduleJobExecDurationEstimate(TDuration duration) override;
    virtual TDuration ExtractScheduleJobExecDurationEstimate() override;

private:
    const int NodeShardId_;
    const TSchedulerConfigPtr Config_;
    const TExecNodePtr Node_;
    const TExecNodeDescriptorPtr NodeDescriptor_;
    const TBooleanFormulaTags NodeTags_;
    const NChunkClient::TMediumDirectoryPtr MediumDirectory_;
    const TJobResources DefaultMinSpareJobResources_;

    TJobResourcesWithQuota UnconditionalDiscount_;
    TJobResourcesWithQuota MaxConditionalDiscount_;

    TJobResources ResourceUsage_;
    TJobResources ResourceLimits_;

    NNodeTrackerClient::NProto::TDiskResources DiskResources_;
    std::optional<int> DiscountMediumIndex_;

    std::vector<TJobPtr> StartedJobs_;
    std::vector<TJobPtr> RunningJobs_;
    std::vector<TPreemptedJob> PreemptedJobs_;

    std::vector<TDiskQuota> DiskRequests_;
    THashMap<TJobId, int> DiskRequestIndexPerJobId_;

    // TODO(eshcherbin): Should we optimize and use tree index instead of operation ID here?
    THashMap<TOperationId, TJobResourcesWithQuota> ConditionalDiscountMap_;

    TScheduleJobsStatistics SchedulingStatistics_;

    std::optional<TDuration> ScheduleJobExecDurationEstimate_;

    bool CanSatisfyResourceRequest(
        const TJobResources& jobResources,
        const TJobResources& conditionalDiscount) const;

    TDiskQuota GetDiskQuotaWithCompactedDefaultMedium(TDiskQuota diskQuota) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
