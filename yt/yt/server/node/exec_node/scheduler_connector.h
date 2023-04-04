#pragma once

#include "private.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TSpecFetchFailedAllocationInfo
{
    TOperationId OperationId;
    TError Error;
};

struct TSchedulerHeartbeatContext
    : public TRefCounted
{
    THashSet<TJobPtr> JobsToForcefullySend;
    THashSet<TJobId> UnconfirmedJobIds;

    THashMap<TAllocationId, TSpecFetchFailedAllocationInfo> SpecFetchFailedAllocations;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerHeartbeatContext)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConnector
    : public TRefCounted
{
public:
    TSchedulerConnector(
        TSchedulerConnectorConfigPtr config,
        IBootstrap* bootstrap);

    void Start();

    void OnDynamicConfigChanged(
        const TExecNodeDynamicConfigPtr& oldConfig,
        const TExecNodeDynamicConfigPtr& newConfig);

    void SetMinSpareResources(const NScheduler::TJobResources& minSpareResources);

    void EnqueueFinishedJobs(std::vector<TJobPtr> jobs);
    void AddUnconfirmedJobs(const std::vector<TJobId>& unconfirmedJobIds);

    void EnqueueSpecFetchFailedAllocation(TAllocationId allocationId, TSpecFetchFailedAllocationInfo info);
    void RemoveSpecFetchFailedAllocations(THashMap<TAllocationId, TSpecFetchFailedAllocationInfo> allocations);

    using TRspHeartbeat = NRpc::TTypedClientResponse<
        NScheduler::NProto::NNode::TRspHeartbeat>;
    using TReqHeartbeat = NRpc::TTypedClientRequest<
        NScheduler::NProto::NNode::TReqHeartbeat,
        TRspHeartbeat>;
    using TRspHeartbeatPtr = TIntrusivePtr<TRspHeartbeat>;
    using TReqHeartbeatPtr = TIntrusivePtr<TReqHeartbeat>;

private:
    const TSchedulerConnectorConfigPtr StaticConfig_;
    TSchedulerConnectorConfigPtr CurrentConfig_;
    IBootstrap* const Bootstrap_;

    NScheduler::TJobResources MinSpareResources_{};

    std::atomic<bool> SendHeartbeatOnJobFinished_{true};

    const NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;

    struct THeartbeatInfo
    {
        TInstant LastSentHeartbeatTime;
        TInstant LastFullyProcessedHeartbeatTime;
        TInstant LastThrottledHeartbeatTime;
        TInstant LastFailedHeartbeatTime;
        TDuration FailedHeartbeatBackoffTime;
    };

    THeartbeatInfo HeartbeatInfo_;

    NProfiling::TEventTimer TimeBetweenSentHeartbeatsCounter_;
    NProfiling::TEventTimer TimeBetweenAcknowledgedHeartbeatsCounter_;
    NProfiling::TEventTimer TimeBetweenFullyProcessedHeartbeatsCounter_;

    THashSet<TJobPtr> JobsToForcefullySend_;
    THashSet<TJobId> UnconfirmedJobIds_;

    THashMap<TAllocationId, TSpecFetchFailedAllocationInfo> SpecFetchFailedAllocations_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void SendHeartbeat();

    void DoSendHeartbeat();

    void OnJobFinished(const TJobPtr& job);

    void OnResourcesAcquired();
    void OnResourcesReleased(
        NJobAgent::EResourcesConsumerType resourcesConsumerType,
        bool fullyReleased);

    void SendOutOfBandHeartbeatIfNeeded();
    void DoSendOutOfBandHeartbeatIfNeeded();

    void PrepareHeartbeatRequest(
        const TReqHeartbeatPtr& request,
        const TSchedulerHeartbeatContextPtr& context);
    void ProcessHeartbeatResponse(
        const TRspHeartbeatPtr& response,
        const TSchedulerHeartbeatContextPtr& context);

    void DoPrepareHeartbeatRequest(
        const TReqHeartbeatPtr& request,
        const TSchedulerHeartbeatContextPtr& context);
    void DoProcessHeartbeatResponse(
        const TRspHeartbeatPtr& response,
        const TSchedulerHeartbeatContextPtr& context);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
