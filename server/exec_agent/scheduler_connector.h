#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConnector
    : public TRefCounted
{
public:
    TSchedulerConnector(
        TSchedulerConnectorConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    void Start();

private:
    const TSchedulerConnectorConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;
    const IInvokerPtr ControlInvoker_;
    TInstant LastSentHeartbeatTime_;
    TInstant LastFullyProcessedHeartbeatTime_;
    TInstant LastThrottledHeartbeatTime_;
    TInstant LastFailedHeartbeatTime_;
    TDuration FailedHeartbeatBackoffTime_;

    NProfiling::TAggregateGauge TimeBetweenSentHeartbeatsCounter_;
    NProfiling::TAggregateGauge TimeBetweenAcknowledgedHeartbeatsCounter_;
    NProfiling::TAggregateGauge TimeBetweenFullyProcessedHeartbeatsCounter_;

    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;

    void SendHeartbeat();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
