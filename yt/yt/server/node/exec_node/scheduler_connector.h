#pragma once

#include "private.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

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

    THashMap<TAllocationId, TSpecFetchFailedAllocationInfo> SpecFetchFailedAllocations;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerHeartbeatContext)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConnector
    : public TRefCounted
{
public:
    explicit TSchedulerConnector(IBootstrap* bootstrap);

    void Start();

    void OnDynamicConfigChanged(
        const TSchedulerConnectorDynamicConfigPtr& oldConfig,
        const TSchedulerConnectorDynamicConfigPtr& newConfig);

    void SetMinSpareResources(const NScheduler::TJobResources& minSpareResources);

    void EnqueueFinishedJobs(std::vector<TJobPtr> jobs);

    void RemoveSpecFetchFailedAllocations(THashMap<TAllocationId, TSpecFetchFailedAllocationInfo> allocations);

    using TRspHeartbeat = NRpc::TTypedClientResponse<
        NScheduler::NProto::NNode::TRspHeartbeat>;
    using TReqHeartbeat = NRpc::TTypedClientRequest<
        NScheduler::NProto::NNode::TReqHeartbeat,
        TRspHeartbeat>;
    using TRspHeartbeatPtr = TIntrusivePtr<TRspHeartbeat>;
    using TReqHeartbeatPtr = TIntrusivePtr<TReqHeartbeat>;

private:
    IBootstrap* const Bootstrap_;

    TAtomicIntrusivePtr<const TSchedulerConnectorDynamicConfig> DynamicConfig_;

    NScheduler::TJobResources MinSpareResources_{};

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

    void OnJobRegistrationFailed(
        TAllocationId allocationId,
        TOperationId operationId,
        const TControllerAgentDescriptor& agentDescriptor,
        const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
