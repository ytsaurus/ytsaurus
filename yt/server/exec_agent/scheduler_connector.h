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
    TSchedulerConnectorConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;
    IInvokerPtr ControlInvoker;

    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor;

    void SendHeartbeat();
    void OnHeartbeatResponse(const NJobTrackerClient::TJobTrackerServiceProxy::TErrorOrRspHeartbeatPtr& rspOrError);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
