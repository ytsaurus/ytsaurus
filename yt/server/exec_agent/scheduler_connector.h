#pragma once

#include "public.h"

#include <ytlib/misc/periodic_invoker.h>

#include <ytlib/scheduler/scheduler_proxy.h>

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
    typedef TSchedulerConnector TThis;

    TSchedulerConnectorConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;
    IInvokerPtr ControlInvoker;

    NScheduler::TSchedulerServiceProxy Proxy;
    TPeriodicInvokerPtr HeartbeatInvoker;

    void SendHeartbeat();
    void OnHeartbeatResponse(NScheduler::TSchedulerServiceProxy::TRspHeartbeatPtr rsp);

    // Non-const reference to swap out job spec.
    void StartJob(NScheduler::NProto::TJobStartInfo& info);
    void AbortJob(const TJobId& jobId);
    void RemoveJob(const TJobId& jobId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
