#pragma once

#include "public.h"

#include <core/concurrency/periodic_executor.h>

#include <ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <server/cell_node/public.h>

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

    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;


    void SendHeartbeat();

};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
