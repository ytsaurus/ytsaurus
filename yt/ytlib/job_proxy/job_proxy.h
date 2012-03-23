#pragma once

#include "public.h"
#include "private.h"
#include "pipes.h"
#include "job_spec.h"

#include <ytlib/job_proxy/public.h>
#include <ytlib/scheduler/public.h>
#include <ytlib/exec_agent/public.h>
#include <ytlib/exec_agent/supervisor_service_proxy.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxy
    : public TNonCopyable
{
public:
    TJobProxy(
        TJobProxyConfig* config,
        const NScheduler::TJobId& jobId);

    void Run();

private:
    void GetJobSpec();
    void InitPipes();
    void ReportStatistic();
    void DoJobIO();

    // Called from forked process.
    void StartJob();

    void ReportResult(const NScheduler::NProto::TJobResult& result);

    typedef NExecAgent::TSupervisorServiceProxy TProxy;

    TJobProxyConfigPtr Config;
    TProxy Proxy;

    const NExecAgent::TJobId JobId;
    TAutoPtr<TJobSpec> JobSpec;

    yvector<IDataPipe::TPtr> DataPipes;
    int ActivePipesCount;

    //TJobStats JobStats;
    TError JobExitStatus;

    int ProcessId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSupervisor
} // namespace NYT
