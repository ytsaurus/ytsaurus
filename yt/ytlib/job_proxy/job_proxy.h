#pragma once

#include "public.h"
#include "private.h"
#include "pipes.h"
#include "job.h"

#include <ytlib/job_proxy/public.h>
#include <ytlib/scheduler/public.h>
#include <ytlib/exec_agent/public.h>
#include <ytlib/exec_agent/supervisor_service_proxy.h>
#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxy
    : public TRefCounted
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

    NScheduler::NProto::TJobSpec GetJobSpec();
    void ReportResult(const NScheduler::NProto::TJobResult& result);

    TJobProxyConfigPtr Config;
    THolder<NExecAgent::TSupervisorServiceProxy> SupervisorProxy;
    NScheduler::TJobId JobId;
    NLog::TTaggedLogger Logger;

    TAutoPtr<IJob> Job;
    TPeriodicInvoker::TPtr HeartbeatInvoker;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSupervisor
} // namespace NYT
