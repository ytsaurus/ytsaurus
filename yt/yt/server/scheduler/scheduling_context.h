#pragma once

#include "private.h"

#include <yt/yt/server/lib/scheduler/scheduling_tag.h>

#include <yt/yt/server/lib/controller_agent/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Refactor TScheduleJobsStatistics to allow for different scheduling strategies.
struct TScheduleJobsStatistics
{
    int ControllerScheduleJobCount = 0;
    int ControllerScheduleJobTimedOutCount = 0;
    TEnumIndexedVector<EJobSchedulingStage, int> ScheduleJobAttemptCountPerStage;
    int MaxNonPreemptiveSchedulingIndex = -1;
    int ScheduledDuringPreemption = 0;
    int UnconditionallyPreemptibleJobCount = 0;
    int TotalConditionallyPreemptibleJobCount = 0;
    int MaxConditionallyPreemptibleJobCountInPool = 0;
    bool ScheduleWithPreemption = false;
    TEnumIndexedVector<EOperationPreemptionPriority, int> OperationCountByPreemptionPriority;
    TJobResources ResourceLimits;
    TJobResources ResourceUsage;
    TJobResources UnconditionalResourceUsageDiscount;
    TJobResources MaxConditionalResourceUsageDiscount;
    bool SsdPriorityPreemptionEnabled = false;
    THashSet<int> SsdPriorityPreemptionMedia;
};

void Serialize(const TScheduleJobsStatistics& statistics, NYson::IYsonConsumer* consumer);

TString FormatPreemptibleInfoCompact(const TScheduleJobsStatistics& statistics);
TString FormatScheduleJobAttemptsCompact(const TScheduleJobsStatistics& statistics);

TString FormatOperationCountByPreemptionPriorityCompact(
    const TEnumIndexedVector<EOperationPreemptionPriority, int>& operationsPerPriority);

////////////////////////////////////////////////////////////////////////////////

struct TPreemptedJob
{
    TJobPtr Job;
    TDuration PreemptionTimeout;
    EJobPreemptionReason PreemptionReason;
};

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingContext
    : public virtual TRefCounted
{
    virtual int GetNodeShardId() const = 0;

    virtual const TExecNodeDescriptorPtr& GetNodeDescriptor() const = 0;

    virtual const TJobResources& ResourceLimits() const = 0;
    virtual TJobResources& ResourceUsage() = 0;
    virtual const NNodeTrackerClient::NProto::TDiskResources& DiskResources() const = 0;
    virtual const std::vector<TDiskQuota>& DiskRequests() const = 0;

    //! Used during preemption to allow second-chance scheduling.
    virtual void ResetDiscounts() = 0;
    virtual const TJobResourcesWithQuota& UnconditionalDiscount() const = 0;
    virtual void IncreaseUnconditionalDiscount(const TJobResourcesWithQuota& jobResources) = 0;
    virtual TJobResourcesWithQuota GetMaxConditionalDiscount() const = 0;
    virtual TJobResourcesWithQuota GetConditionalDiscountForOperation(TOperationId operationId) const = 0;
    virtual void SetConditionalDiscountForOperation(TOperationId operationId, const TJobResourcesWithQuota& discount) = 0;
    virtual NNodeTrackerClient::NProto::TDiskResources GetDiskResourcesWithDiscountForOperation(TOperationId operationId) const = 0;
    virtual TJobResources GetNodeFreeResourcesWithoutDiscount() const = 0;
    virtual TJobResources GetNodeFreeResourcesWithDiscount() const = 0;
    virtual TJobResources GetNodeFreeResourcesWithDiscountForOperation(TOperationId operationId) const = 0;

    virtual const std::vector<TJobPtr>& StartedJobs() const = 0;
    virtual const std::vector<TJobPtr>& RunningJobs() const = 0;
    virtual const std::vector<TPreemptedJob>& PreemptedJobs() const = 0;

    //! Returns |true| if node has enough resources to start job with given limits.
    virtual bool CanStartJobForOperation(const TJobResourcesWithQuota& jobResources, TOperationId operationId) const = 0;
    //! Returns |true| if any more new jobs can be scheduled at this node.
    virtual bool CanStartMoreJobs(
        const std::optional<TJobResources>& customMinSpareJobResources = {}) const = 0;
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
        EPreemptionMode preemptionMode,
        int schedulingIndex,
        EJobSchedulingStage schedulingStage) = 0;

    virtual void PreemptJob(const TJobPtr& job, TDuration preemptionTimeout, EJobPreemptionReason preemptionReason) = 0;

    virtual NProfiling::TCpuInstant GetNow() const = 0;

    virtual TScheduleJobsStatistics GetSchedulingStatistics() const = 0;
    virtual void SetSchedulingStatistics(TScheduleJobsStatistics statistics) = 0;

    virtual void StoreScheduleJobExecDurationEstimate(TDuration duration) = 0;
    virtual TDuration ExtractScheduleJobExecDurationEstimate() = 0;
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
