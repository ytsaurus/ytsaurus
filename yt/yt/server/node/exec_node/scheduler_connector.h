#pragma once

#include "private.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/core/concurrency/retrying_periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TFailedAllocationInfo
{
    TOperationId OperationId;
    TError Error;
};

struct TSchedulerHeartbeatContext
    : public TRefCounted
{
    THashSet<TJobPtr> JobsToForcefullySend;

    THashMap<TAllocationId, TFailedAllocationInfo> FailedAllocations;
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

    using TRspHeartbeat = NRpc::TTypedClientResponse<
        NScheduler::NProto::NNode::TRspHeartbeat>;
    using TReqHeartbeat = NRpc::TTypedClientRequest<
        NScheduler::NProto::NNode::TReqHeartbeat,
        TRspHeartbeat>;
    using TRspHeartbeatPtr = TIntrusivePtr<TRspHeartbeat>;
    using TReqHeartbeatPtr = TIntrusivePtr<TReqHeartbeat>;

private:
    IBootstrap* const Bootstrap_;

    TAtomicIntrusivePtr<TSchedulerConnectorDynamicConfig> DynamicConfig_;

    NScheduler::TJobResources MinSpareResources_{};

    const NConcurrency::TRetryingPeriodicExecutorPtr HeartbeatExecutor_;

    struct THeartbeatInfo
    {
        TInstant LastSentHeartbeatTime;
        TInstant LastFullyProcessedHeartbeatTime;
        TInstant LastThrottledHeartbeatTime;
    };

    THeartbeatInfo HeartbeatInfo_;

    NProfiling::TEventTimer TimeBetweenSentHeartbeatsCounter_;
    NProfiling::TEventTimer TimeBetweenAcknowledgedHeartbeatsCounter_;
    NProfiling::TEventTimer TimeBetweenFullyProcessedHeartbeatsCounter_;

    THashSet<TJobPtr> JobsToForcefullySend_;

    THashMap<TAllocationId, TFailedAllocationInfo> FailedAllocations_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    TError SendHeartbeat();

    TError DoSendHeartbeat();

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

    void EnqueueFinishedJobs(std::vector<TJobPtr> jobs);
    void RemoveSentJobs(const THashSet<TJobPtr>& jobs);

    void RemoveFailedAllocations(THashMap<TAllocationId, TFailedAllocationInfo> allocations);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
