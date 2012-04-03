#pragma once

#include "public.h"

#include <ytlib/scheduler/scheduler_proxy.h>

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
    
    NScheduler::TSchedulerServiceProxy Proxy;

    void ScheduleHeartbeat();
    void SendHeartbeat();
    void OnHeartbeatResponse(NScheduler::TSchedulerServiceProxy::TRspHeartbeat::TPtr rsp);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
