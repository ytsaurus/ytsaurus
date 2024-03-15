#pragma once

#include "private.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/core/concurrency/retrying_periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerHeartbeatContext
    : public TRefCounted
{
    THashSet<TAllocationPtr> FinishedAllocations;
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

    void EnqueueFinishedAllocation(TAllocationPtr allocation);

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

    THashSet<TAllocationPtr> FinishedAllocations_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    TError SendHeartbeat();

    TError DoSendHeartbeat();

    void OnResourcesAcquired();
    void OnResourcesReleased(
        NJobAgent::EResourcesConsumerType resourcesConsumerType,
        bool fullyReleased);

    void OnAllocationFinished(TAllocationPtr allocation);

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

    void RemoveSentAllocations(const THashSet<TAllocationPtr>& allocations);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
