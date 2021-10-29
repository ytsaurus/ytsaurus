#pragma once

#include "public.h"

#include <yt/yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/profiling/profiler.h>

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

private:
    const TSchedulerConnectorConfigPtr StaticConfig_;
    TSchedulerConnectorConfigPtr CurrentConfig_;
    IBootstrap* const Bootstrap_;

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

    void SendHeartbeat() noexcept;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
