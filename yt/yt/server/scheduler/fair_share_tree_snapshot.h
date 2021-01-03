#pragma once

#include <yt/server/lib/scheduler/resource_metering.h>

#include <yt/ytlib/scheduler/job_resources.h>

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
    virtual bool IsOperationRunningInTree(TOperationId operationId) const = 0;
    virtual bool IsOperationDisabled(TOperationId operationId) const = 0;
    virtual void ApplyJobMetricsDelta(TOperationId operationId, const TJobMetrics& jobMetricsDelta) = 0;
    virtual const TSchedulingTagFilter& GetNodesFilter() const = 0;
    virtual TJobResources GetTotalResourceLimits() const = 0;
    virtual std::optional<TSchedulerElementStateSnapshot> GetMaybeStateSnapshotForPool(const TString& poolId) const = 0;
    virtual void BuildResourceMetering(TMeteringMap* statistics) const = 0;
    virtual void ProfileFairShare() const = 0;
    virtual void LogFairShare(NEventLog::TFluentLogEvent fluent) const = 0;
    virtual void EssentialLogFairShare(NEventLog::TFluentLogEvent fluent) const = 0;
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
