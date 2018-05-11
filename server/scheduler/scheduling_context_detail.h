#pragma once

#include "scheduling_context.h"
#include "exec_node.h"

#include <yt/ytlib/scheduler/job_resources.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContextBase
    : public ISchedulingContext
{
public:
    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsageDiscount);
    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TDiskResources, DiskInfo);

    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, StartedJobs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, PreemptedJobs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, RunningJobs);

    DEFINE_BYVAL_RW_PROPERTY(TFairShareSchedulingStatistics, SchedulingStatistics);

public:
    TSchedulingContextBase(
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        const std::vector<TJobPtr>& runningJobs);

    virtual const TExecNodeDescriptor& GetNodeDescriptor() const override;

    virtual bool CanStartJob(const TJobResourcesWithQuota& jobResourcesWithQuota) const override;
    virtual bool CanStartMoreJobs() const override;
    virtual bool CanSchedule(const TSchedulingTagFilter& filter) const override;

    virtual void StartJob(
        const TString& treeId,
        const TOperationId& operationId,
        const TIncarnationId& incarnationId,
        const NControllerAgent::TJobStartDescriptor& startDescriptor) override;

    virtual void PreemptJob(const TJobPtr& job) override;

    virtual TJobResources GetFreeResources() override;

private:
    const TSchedulerConfigPtr Config_;
    const TExecNodePtr Node_;
    const TExecNodeDescriptor NodeDescriptor_;
    const THashSet<TString> NodeTags_;

    bool CanSatisfyResourceRequest(const TJobResources& jobResources) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
