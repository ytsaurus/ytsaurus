#pragma once

#include "common.h"
#include "pipes.h"
#include "job_spec.h"

//#include "job_stats.pb.h"

#include <ytlib/exec_agent/common.h>
#include <ytlib/exec_agent/supervisor_service_proxy.h>
#include <ytlib/transaction_client/transaction.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

// ToDo: move to scheduler namespace.
typedef TGuid TOperationId;

//typedef TValueOrError<NProto::TJobStats> TJobStats;

////////////////////////////////////////////////////////////////////////////////

class TJobProxy
    : public TNonCopyable
{
public:
    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        Stroka SupervisorServiceAddress;
        TDuration RpcTimeout;
        TDuration SelectTimeout;

        TJobSpec::TConfig::TPtr JobSpec;
        Stroka SandboxName;

        TConfig()
        {
            Register("supervisor_service_address", SupervisorServiceAddress);
            Register("rpc_timeout", RpcTimeout).Default(TDuration::Seconds(5));
            Register("select_timeout", SelectTimeout).Default(TDuration::Seconds(1));
            Register("job_spec", JobSpec).DefaultNew();
            Register("sandbox_name", SandboxName).Default("sandbox");
        }
    };

    TJobProxy(
        TConfig* config,
        const TOperationId& operationId, 
        const int jobIndex);

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

    TConfig::TPtr Config;
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
