#pragma once

#include "public.h"

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/misc/delayed_invoker.h>

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
    TDelayedInvoker::TCookie HeartbeatCookie;

    bool HeartbeatInProgress;
    bool OutOfOrderHeartbeatNeeded;

    void ScheduleNextHeartbeat();
    void SendHeartbeat();
    void OnHeartbeatResponse(NScheduler::TSchedulerServiceProxy::TRspHeartbeat::TPtr rsp);

    void StartJob(const NScheduler::NProto::TStartJobInfo& info);
    void AbortJob(const TJobId& jobId);
    void RemoveJob(const TJobId& jobId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
