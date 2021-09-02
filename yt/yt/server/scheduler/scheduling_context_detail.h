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
    
    virtual int GetNodeShardId() const override;

    virtual TJobResources& UnconditionalResourceUsageDiscount() override;
    virtual TJobResources GetMaxConditionalUsageDiscount() const override;
    
    virtual const TJobResources& ResourceLimits() const override;

    const TJobResources& ResourceUsage() const;
    virtual TJobResources& ResourceUsage() override;
    
    virtual const NNodeTrackerClient::NProto::TDiskResources& DiskResources() const override;
    NNodeTrackerClient::NProto::TDiskResources& DiskResources();

    virtual const TExecNodeDescriptor& GetNodeDescriptor() const override;

    virtual bool CanStartJobForOperation(
        const TJobResourcesWithQuota& jobResourcesWithQuota,
        TOperationId operationId) const override;
    virtual bool CanStartMoreJobs() const override;
    virtual bool CanSchedule(const TSchedulingTagFilter& filter) const override;
    virtual bool ShouldAbortJobsSinceResourcesOvercommit() const override;
    
    virtual const std::vector<TJobPtr>& StartedJobs() const override;
    virtual const std::vector<TJobPtr>& RunningJobs() const override;
    virtual const std::vector<TPreemptedJob>& PreemptedJobs() const override;

    virtual void StartJob(
        const TString& treeId,
        TOperationId operationId,
        TIncarnationId incarnationId,
        TControllerEpoch controllerEpoch,
        const TJobStartDescriptor& startDescriptor,
        EPreemptionMode preemptionMode) override;

    virtual void PreemptJob(const TJobPtr& job, TDuration interruptTimeout) override;

    virtual void ResetUsageDiscounts() override;
    virtual void SetConditionalDiscountForOperation(TOperationId operationId, const TJobResources& discount) override;
    virtual TJobResources GetConditionalDiscountForOperation(TOperationId operationId) const override;
    virtual TJobResources GetNodeFreeResourcesWithoutDiscount() const override;
    virtual TJobResources GetNodeFreeResourcesWithDiscount() const override;
    virtual TJobResources GetNodeFreeResourcesWithDiscountForOperation(TOperationId operationId) const override;
    
    virtual TScheduleJobsStatistics GetSchedulingStatistics() const override;
    virtual void SetSchedulingStatistics(TScheduleJobsStatistics statistics) override;

    virtual ESchedulingSegment GetSchedulingSegment() const override;

private:
    const int NodeShardId_;
    const TSchedulerConfigPtr Config_;
    const TExecNodePtr Node_;
    const TExecNodeDescriptor NodeDescriptor_;
    const TBooleanFormulaTags NodeTags_;
    const NChunkClient::TMediumDirectoryPtr MediumDirectory_;
    const TJobResources MinSpareJobResources_;
    
    TJobResources UnconditionalResourceUsageDiscount_;
    TJobResources MaxConditionalUsageDiscount_;
    TJobResources ResourceUsage_;
    TJobResources ResourceLimits_;
    NNodeTrackerClient::NProto::TDiskResources DiskResources_;
    
    std::vector<TJobPtr> StartedJobs_;
    std::vector<TJobPtr> RunningJobs_;
    std::vector<TPreemptedJob> PreemptedJobs_;

    std::vector<TDiskQuota> DiskRequests_;

    // TODO(eshcherbin): Should we optimize and use tree index instead of operation ID here?
    THashMap<TOperationId, TJobResources> ConditionalUsageDiscountMap_;
    
    TScheduleJobsStatistics SchedulingStatistics_;

    bool CanSatisfyResourceRequest(
        const TJobResources& jobResources,
        const TJobResources& conditionalDiscount) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
