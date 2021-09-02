#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/scheduling_tag.h>

#include <yt/yt/server/lib/controller_agent/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobsStatistics
{
    int ControllerScheduleJobCount = 0;
    int AggressivelyPreemptiveScheduleJobAttempts = 0;
    int PreemptiveScheduleJobAttempts = 0;
    int NonPreemptiveScheduleJobAttempts = 0;
    int PackingFallbackScheduleJobAttempts = 0;
    int ScheduledDuringPreemption = 0;
    int UnconditionallyPreemptableJobCount = 0;
    int TotalConditionallyPreemptableJobCount = 0;
    int MaxConditionallyPreemptableJobCountInPool = 0;
    bool ScheduleWithPreemption = false;
    bool HasAggressivelyStarvingElements = false;
    TJobResources ResourceLimits;
    TJobResources ResourceUsage;
    TJobResources UnconditionalResourceUsageDiscount;
    TJobResources MaxConditionalResourceUsageDiscount;
};

void Serialize(const TScheduleJobsStatistics& event, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TPreemptedJob
{
    TJobPtr Job;
    TDuration InterruptTimeout; 
};

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingContext
    : public virtual TRefCounted
{
    virtual int GetNodeShardId() const = 0;

    virtual const TExecNodeDescriptor& GetNodeDescriptor() const = 0;

    virtual const TJobResources& ResourceLimits() const = 0;
    virtual TJobResources& ResourceUsage() = 0;
    virtual const NNodeTrackerClient::NProto::TDiskResources& DiskResources() const = 0;
    //! Used during preemption to allow second-chance scheduling.
    virtual void ResetUsageDiscounts() = 0;
    virtual TJobResources& UnconditionalResourceUsageDiscount() = 0;
    virtual void SetConditionalDiscountForOperation(TOperationId operationId, const TJobResources& discount) = 0;
    virtual TJobResources GetConditionalDiscountForOperation(TOperationId operationId) const = 0;
    virtual TJobResources GetMaxConditionalUsageDiscount() const = 0;
    virtual TJobResources GetNodeFreeResourcesWithoutDiscount() const = 0;
    virtual TJobResources GetNodeFreeResourcesWithDiscount() const = 0;
    virtual TJobResources GetNodeFreeResourcesWithDiscountForOperation(TOperationId operationId) const = 0;

    virtual const std::vector<TJobPtr>& StartedJobs() const = 0;
    virtual const std::vector<TJobPtr>& RunningJobs() const = 0;
    virtual const std::vector<TPreemptedJob>& PreemptedJobs() const = 0;

    //! Returns |true| if node has enough resources to start job with given limits.
    virtual bool CanStartJobForOperation(const TJobResourcesWithQuota& jobResources, TOperationId operationId) const = 0;
    //! Returns |true| if any more new jobs can be scheduled at this node.
    virtual bool CanStartMoreJobs() const = 0;
    //! Returns |true| if the node can handle jobs demanding a certain #tag.
    virtual bool CanSchedule(const TSchedulingTagFilter& filter) const = 0;

    //! Returns |true| if strategy should abort jobs since resources overcommit.
    virtual bool ShouldAbortJobsSinceResourcesOvercommit() const = 0;

    virtual void StartJob(
        const TString& treeId,
        TOperationId operationId,
        TIncarnationId incarnationId,
        TControllerEpoch controllerEpoch,
        const TJobStartDescriptor& startDescriptor,
        EPreemptionMode preemptionMode) = 0;

    virtual void PreemptJob(const TJobPtr& job, TDuration interruptTimeout) = 0;

    virtual NProfiling::TCpuInstant GetNow() const = 0;

    virtual TScheduleJobsStatistics GetSchedulingStatistics() const = 0;
    virtual void SetSchedulingStatistics(TScheduleJobsStatistics statistics) = 0;

    virtual ESchedulingSegment GetSchedulingSegment() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulingContext)

////////////////////////////////////////////////////////////////////////////////

ISchedulingContextPtr CreateSchedulingContext(
    int nodeShardId,
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
