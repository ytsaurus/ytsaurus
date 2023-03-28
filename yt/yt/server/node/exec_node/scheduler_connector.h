#pragma once

#include "public.h"

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NExecNode {

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
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
