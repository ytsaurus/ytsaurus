#pragma once

#include "public.h"
#include "private.h"
#include "pipes.h"
#include "job.h"

#include <ytlib/scheduler/public.h>

#include <ytlib/misc/periodic_invoker.h>

#include <ytlib/logging/tagged_logger.h>

#include <server/exec_agent/public.h>
#include <server/exec_agent/supervisor_service_proxy.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxy
    : public TRefCounted
    , public IJobHost
{
public:
    TJobProxy(
        TJobProxyConfigPtr config,
        const NScheduler::TJobId& jobId);

    //! Runs the job. Blocks until the job is complete.
    void Run();

private:
    void SendHeartbeat();
    void OnHeartbeatResponse(NExecAgent::TSupervisorServiceProxy::TRspOnJobProgressPtr rsp);

    void RetrieveJobSpec();
    void ReportResult(const NScheduler::NProto::TJobResult& result);

    TJobProxyConfigPtr Config;
    THolder<NExecAgent::TSupervisorServiceProxy> SupervisorProxy;
    NScheduler::TJobId JobId;
    NLog::TTaggedLogger Logger;

    TJobPtr Job;
    TPeriodicInvokerPtr HeartbeatInvoker;

    NScheduler::NProto::TJobSpec JobSpec;
    NScheduler::NProto::TNodeResources ResourceUtilization;

    // IJobHost implementation.
    virtual TJobProxyConfigPtr GetConfig() override;
    virtual const NScheduler::NProto::TJobSpec& GetJobSpec() override;

    virtual NScheduler::NProto::TNodeResources GetResourceUtilization() override;
    virtual void SetResourceUtilization(const NScheduler::NProto::TNodeResources& utilization) override;

    virtual void ReleaseNetwork() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
