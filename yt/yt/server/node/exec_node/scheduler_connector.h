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

private:
    const TSchedulerConnectorConfigPtr Config_;
    IBootstrap* const Bootstrap_;

    const NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;

    TInstant LastSentHeartbeatTime_;
    TInstant LastFullyProcessedHeartbeatTime_;
    TInstant LastThrottledHeartbeatTime_;
    TInstant LastFailedHeartbeatTime_;
    TDuration FailedHeartbeatBackoffTime_;

    NProfiling::TEventTimer TimeBetweenSentHeartbeatsCounter_;
    NProfiling::TEventTimer TimeBetweenAcknowledgedHeartbeatsCounter_;
    NProfiling::TEventTimer TimeBetweenFullyProcessedHeartbeatsCounter_;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void SendHeartbeat();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
