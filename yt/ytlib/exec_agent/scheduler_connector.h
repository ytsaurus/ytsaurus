#pragma once

#include "public.h"

#include <ytlib/scheduler/scheduler_service_proxy.h>
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
    TSchedulerConnectorConfigPtr Config;
    TBootstrap* Bootstrap;
    
    THolder<NScheduler::TSchedulerServiceProxy> Proxy;
    TPeriodicInvoker::TPtr PeriodicInvoker;

    void SendHeartbeat();
    void OnHeartbeatResponse(NScheduler::TSchedulerServiceProxy::TRspHeartbeat::TPtr rsp);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
