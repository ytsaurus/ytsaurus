#pragma once

#include "public.h"
#include "scheduling_tag.h"

#include <yt/server/controller_agent/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/profiling/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TFairShareSchedulingStatistics
{
    int ControllerScheduleJobCount = 0;
    int PreemptiveScheduleJobAttempts = 0;
    int NonPreemptiveScheduleJobAttempts = 0;
    int ScheduledDuringPreemption = 0;
    int PreemptableJobCount = 0;
    bool HasAggressivelyStarvingNodes = false;
    TJobResources ResourceUsageDiscount;
};

struct ISchedulingContext
    : public virtual TRefCounted
{
    virtual const TExecNodeDescriptor& GetNodeDescriptor() const = 0;

    virtual const TJobResources& ResourceLimits() const = 0;
    virtual TJobResources& ResourceUsage() = 0;
    virtual const NNodeTrackerClient::NProto::TDiskResources& DiskInfo() const = 0;
    //! Used during preemption to allow second-chance scheduling.
    virtual TJobResources& ResourceUsageDiscount() = 0;
    virtual TJobResources GetFreeResources() = 0;

    virtual const std::vector<TJobPtr>& StartedJobs() const = 0;
    virtual const std::vector<TJobPtr>& PreemptedJobs() const = 0;
    virtual const std::vector<TJobPtr>& RunningJobs() const = 0;

    //! Returns |true| if node has enough resources to start job with given limits.
    virtual bool CanStartJob(const TJobResourcesWithQuota& jobResources) const = 0;
    //! Returns |true| if any more new jobs can be scheduled at this node.
    virtual bool CanStartMoreJobs() const = 0;
    //! Returns |true| if the node can handle jobs demanding a certain #tag.
    virtual bool CanSchedule(const TSchedulingTagFilter& filter) const = 0;

    virtual void StartJob(
        const TString& treeId,
        const TOperationId& operationId,
        const TIncarnationId& incarnationId,
        const NControllerAgent::TJobStartDescriptor& startDescriptor) = 0;

    virtual void PreemptJob(const TJobPtr& job) = 0;

    virtual NProfiling::TCpuInstant GetNow() const = 0;

    virtual TFairShareSchedulingStatistics GetSchedulingStatistics() const = 0;
    virtual void SetSchedulingStatistics(TFairShareSchedulingStatistics statistics) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulingContext)

ISchedulingContextPtr CreateSchedulingContext(
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
