#include "stdafx.h"

#include "config.h"
#include "job_proxy.h"
#include "user_job.h"
#include "merge_job.h"


#include <ytlib/rpc/channel.h>
//#include <ytlib/misc/linux.h>


namespace NYT {
namespace NJobProxy {

using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyConfigPtr config,
    const TJobId& jobId)
    : Config(config)
    , Proxy(~NRpc::CreateBusChannel(config->ExecAgentAddress))
    , JobId(jobId)
{
    PingInvoker = New<TPeriodicInvoker>(
        BIND(&TJobProxy::SendHeartbeat, this), 
        config->HeartbeatPeriod);

    Proxy.SetDefaultTimeout(config->RpcTimeout);
}

void TJobProxy::SendHeartbeat()
{
    auto req = Proxy.OnProgress();
    *req->mutable_job_id() = JobId.ToProto();

    auto rsp = req->Invoke()->Get();


    if (!rsp->IsOK()) {
        // NB: user process is not killed here.
        // Good user processes are supposed to die themselves 
        // when io pipes are closed.
        // Bad processes will die at container shutdown.
        LOG_FATAL("Failed to report progress for job %s", ~JobId.ToString());
    }
}

TJobSpec TJobProxy::GetJobSpec()
{
    LOG_DEBUG("Requesting spec for job %s", ~JobId.ToString());
    auto req = Proxy.GetJobSpec();
    *req->mutable_job_id() = JobId.ToProto();

    auto rsp = req->Invoke()->Get();

    if (!rsp->IsOK()) {
        ythrow yexception() << Sprintf("Failed to get job spec (JobId: %s, Error: %s)",
            ~JobId.ToString(),
            ~rsp->GetError().ToString());
    }

    return rsp->job_spec();
}

void TJobProxy::Start()
{
    try {
        auto jobSpec = GetJobSpec();

        if (jobSpec.HasExtension(TUserJobSpec::user_job_spec)) {
            Job = new TUserJob(Config, jobSpec);
        } else if (jobSpec.HasExtension(TMergeJobSpec::merge_job_spec)) {
            Job = new TMergeJob(
                Config->JobIo, 
                Config->Masters, 
                jobSpec.GetExtension(TMergeJobSpec::merge_job_spec));
        }

        auto result = Job->Run();
        ReportResult(result);

    } catch (const std::exception& ex) {
        LOG_DEBUG(
            "Job failed (JobId: %s, error: %s)", 
            ~JobId.ToString(),
            ex.what());

        TJobResult result;
        result.mutable_error()->set_code(TError::Fail);
        result.mutable_error()->set_message(ex.what());
        ReportResult(result);
    }
}

void TJobProxy::ReportResult(
    const NScheduler::NProto::TJobResult& result)
{
    PingInvoker->Stop();

    auto req = Proxy.OnJobFinished();
    *(req->mutable_result()) = result;
    *(req->mutable_job_id()) = JobId.ToProto();

    auto rsp = req->Invoke()->Get();
    if (!rsp->IsOK()) {
        LOG_FATAL("Failed to report result for job %s", ~JobId.ToString());
        // log error, use some exotic exit status.
        exit(1);
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
