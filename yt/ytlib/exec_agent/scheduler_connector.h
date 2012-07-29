#pragma once

#include "public.h"

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/misc/periodic_invoker.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConnector
    : public TRefCounted
{
public:
    TSchedulerConnector(
        TSchedulerConnectorConfigPtr config,    
        TBootstrap* bootstrap);

    void Start();

private:
    typedef TSchedulerConnector TThis;

    TSchedulerConnectorConfigPtr Config;
    TBootstrap* Bootstrap;
    IInvokerPtr ControlInvoker;

    NScheduler::TSchedulerServiceProxy Proxy;
    TPeriodicInvokerPtr HeartbeatInvoker;

    void SendHeartbeat();
    void OnHeartbeatResponse(NScheduler::TSchedulerServiceProxy::TRspHeartbeatPtr rsp);

    void StartJob(const NScheduler::NProto::TJobStartInfo& info);
    void AbortJob(const TJobId& jobId);
    void RemoveJob(const TJobId& jobId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
