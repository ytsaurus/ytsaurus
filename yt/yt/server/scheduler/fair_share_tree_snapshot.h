#pragma once

#include "scheduler_strategy.h"
#include "private.h"

#include <yt/yt/server/lib/scheduler/resource_metering.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerElementStateSnapshot
{
    TResourceVector DemandShare;
    TResourceVector PromisedFairShare;
};

////////////////////////////////////////////////////////////////////////////////

//! Thread affinity: any
struct IFairShareTreeSnapshot
    : public TRefCounted
{
    virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) = 0;
    virtual void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext) = 0;
    virtual void ProcessUpdatedJob(
        TOperationId operationId,
        TJobId jobId,
        const TJobResources& jobResources,
        const TDataCenter& jobDataCenter,
        bool* shouldAbortJob) = 0;
    virtual void ProcessFinishedJob(TOperationId operationId, TJobId jobId) = 0;
    virtual bool HasOperation(TOperationId operationId) const = 0;
    virtual bool HasEnabledOperation(TOperationId operationId) const = 0;
    virtual bool HasDisabledOperation(TOperationId operationId) const = 0;
    virtual bool IsOperationRunningInTree(TOperationId operationId) const = 0;
    virtual void ApplyJobMetricsDelta(const THashMap<TOperationId, TJobMetrics>& jobMetricsPerOperation) = 0;
    virtual void ApplyScheduledAndPreemptedResourcesDelta(
        const THashMap<std::optional<EJobSchedulingStage>, TOperationIdToJobResources>& scheduledJobResources,
        const TEnumIndexedVector<EJobPreemptionReason, TOperationIdToJobResources>& preemptedJobResources,
        const TEnumIndexedVector<EJobPreemptionReason, TOperationIdToJobResources>& preemptedJobResourceTimes) = 0;
    virtual const TFairShareStrategyTreeConfigPtr& GetConfig() const = 0;
    virtual const TSchedulingTagFilter& GetNodesFilter() const = 0;
    virtual TJobResources GetTotalResourceLimits() const = 0;
    virtual std::optional<TSchedulerElementStateSnapshot> GetMaybeStateSnapshotForPool(const TString& poolId) const = 0;
    virtual void BuildResourceMetering(TMeteringMap* meteringMap) const = 0;
    virtual TCachedJobPreemptionStatuses GetCachedJobPreemptionStatuses() const = 0;
    virtual void ProfileFairShare() const = 0;
    virtual void LogFairShareAt(TInstant now) const = 0;
    virtual void EssentialLogFairShareAt(TInstant now) const = 0;
    virtual void UpdateResourceUsageSnapshot() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareTreeSnapshot);

////////////////////////////////////////////////////////////////////////////////

//! This interface must be thread-safe.
struct IFairShareTreeHost
    : public virtual TRefCounted
{
    virtual TResourceTree* GetResourceTree() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareTreeHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
