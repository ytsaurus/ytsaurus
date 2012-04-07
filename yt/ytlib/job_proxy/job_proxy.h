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

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxy
    : public TNonCopyable
{
public:
    TJobProxy(
        TJobProxyConfigPtr config,
        const NScheduler::TJobId& jobId);

    void Start();

private:
    void SendHeartbeat();

    NScheduler::NProto::TJobSpec GetJobSpec();
    void ReportResult(const NScheduler::NProto::TJobResult& result);

    typedef NExecAgent::TSupervisorServiceProxy TProxy;

    TJobProxyConfigPtr Config;
    TProxy Proxy;
    TAutoPtr<IJob> Job;

    const NScheduler::TJobId JobId;

    TPeriodicInvoker::TPtr HeartbeatInvoker;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSupervisor
} // namespace NYT
