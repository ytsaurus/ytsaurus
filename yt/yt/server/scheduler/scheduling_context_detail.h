#pragma once

#include "scheduling_context.h"
#include "exec_node.h"

#include <yt/ytlib/scheduler/job_resources.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContextBase
    : public ISchedulingContext
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, NodeShardId);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsageDiscount);
    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TDiskResources, DiskResources);

    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, StartedJobs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, PreemptedJobs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, GracefullyPreemptedJobs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, RunningJobs);

    DEFINE_BYVAL_RW_PROPERTY(TFairShareSchedulingStatistics, SchedulingStatistics);

public:
    TSchedulingContextBase(
        int nodeShardId,
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        const std::vector<TJobPtr>& runningJobs,
        const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

    virtual const TExecNodeDescriptor& GetNodeDescriptor() const override;

    virtual bool CanStartJob(const TJobResourcesWithQuota& jobResourcesWithQuota) const override;
    virtual bool CanStartMoreJobs() const override;
    virtual bool CanSchedule(const TSchedulingTagFilter& filter) const override;
    virtual bool ShouldAbortJobsSinceResourcesOvercommit() const override;

    virtual void StartJob(
        const TString& treeId,
        TOperationId operationId,
        TIncarnationId incarnationId,
        const TJobStartDescriptor& startDescriptor,
        EPreemptionMode preemptionMode) override;

    virtual void PreemptJob(const TJobPtr& job) override;
    virtual void PreemptJobGracefully(const TJobPtr& job) override;

    virtual TJobResources GetNodeFreeResourcesWithoutDiscount() override;
    virtual TJobResources GetNodeFreeResourcesWithDiscount() override;

private:
    const TSchedulerConfigPtr Config_;
    const TExecNodePtr Node_;
    const TExecNodeDescriptor NodeDescriptor_;
    const THashSet<TString> NodeTags_;
    const NChunkClient::TMediumDirectoryPtr MediumDirectory_;

    std::vector<TDiskQuota> DiskRequests_;

    bool CanSatisfyResourceRequest(const TJobResources& jobResources) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
